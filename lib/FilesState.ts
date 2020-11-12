import fs from "fs";
import glob from "glob";
import { EventEmitter } from "events";
import watch from "node-watch";
import path from "path";
import { FileParser } from "./parser";
import { IFile } from "./interface";
import { DatabaseTrigger, DatabaseFunction, Cache } from "./ast";
import { flatMap } from "lodash";
import { Diff } from "./Diff";

export class FilesState extends EventEmitter {
    static create(params: {folder: string | string[], onError?: any}) {

        const filesState = new FilesState({
            folder: params.folder
        });

        if ( params.onError ) {
            filesState.on("error", params.onError);
        }

        filesState.parse();

        return filesState;
    }

    private folders: string[];
    private files: IFile[];
    private fsWatcher?: ReturnType<typeof watch>;

    private fileParser: FileParser;
    
    constructor(params: {folder: string | string[]}) {
        super();

        this.fileParser = new FileParser();

        if ( typeof params.folder === "string" ) {
            this.folders = [params.folder];
        }
        else {
            this.folders = params.folder;
        }

        this.files = [];
    }

    getFolders() {
        return this.folders;
    }

    getFunctions() {
        const outFunctions = flatMap(this.files, file =>
            file.content.functions
        );
        return outFunctions;
    }

    getTriggers() {
        const outTriggers = flatMap(this.files, file =>
            file.content.triggers
        );
        return outTriggers;
    }

    getCache(): Cache[] {
        const outCache = flatMap(this.files, file =>
            file.content.cache || []
        );
        return outCache;
    }

    getFiles() {
        return this.files;
    }

    async watch() {
        const handler = (eventType: string, rootFolder: string, subPath: string) => {
            try {
                this.onChangeWatcher(eventType, rootFolder, subPath);
            } catch(err) {
                this.emitError({
                    subPath,
                    err
                });
            }
        };

        const promise = Promise.all(this.folders.map(rootFolder =>
            new Promise((resolve) => {
                this.fsWatcher = watch(rootFolder, {
                    recursive: true,
                    delay: 5
                }, (eventType, fullPath) => {
                    const subPath = path.relative(rootFolder, fullPath);
    
                    handler(eventType, rootFolder, subPath);
                });
    
                this.fsWatcher.on("ready", () => {
                    resolve();
                });
            })
        ));

        return promise;
    }

    stopWatch() {
        if ( this.fsWatcher ) {
            this.fsWatcher.close();
            delete this.fsWatcher;
        }
    }
    
    private parse() {
        for (const folderPath of this.folders) {
            this.parseFolder(folderPath);
        }
    }

    private parseFolder(folderPath: string) {

        if ( !fs.existsSync(folderPath) ) {
            throw new Error(`folder "${ folderPath }" not found`);
        }

        // fill this.files, make array of object:
        // {
        //   name: "some-file-name.sql",
        //   path: "/path/to/some-file-name.sql",
        //   content: {
        //        functions: [],
        //        triggers: []
        //   }
        // }

        const files = glob.sync(folderPath + "/**/*.sql");
        files.forEach(filePath => {
            // ignore dirs with *.sql name
            //   ./dir.sql/file
            const stat = fs.lstatSync(filePath);
            if ( !stat.isFile() ) {
                return;
            }

            let file;
            try {
                file = this.parseFile(folderPath, filePath);

                if ( file && file.content ) {
                    this.checkDuplicate( file );
                }
            } catch(err) {
                this.emitError({
                    subPath: filePath.slice( folderPath.length + 1 ),
                    err
                });
            }
            
            if ( file ) {
                this.files.push(file);
            }
        });
    }

    private checkDuplicate(file: IFile) {
        const content = file.content;

        content.functions.forEach(func => 
            this.checkDuplicateFunction( func )
        );

        if ( content.triggers ) {
            content.triggers.forEach(trigger => {
                this.checkDuplicateTrigger( trigger );
            });
        }
    }

    private checkDuplicateFunction(func: DatabaseFunction) {
        const identify = func.getSignature();

        const hasDuplicate = this.files.some(someFile => {
            return someFile.content.functions.some((someFunc) => {
                const someIdentify = someFunc.getSignature();
                
                return identify === someIdentify;
            });
        });

        if ( hasDuplicate ) {
            throw new Error(`duplicate function ${ identify }`);
        }
    }

    private checkDuplicateTrigger(trigger: DatabaseTrigger) {
        const identify = trigger.getSignature();

        const hasDuplicate = this.files.some(someFile => {
            const someTriggers = someFile.content.triggers;

            if ( someTriggers ) {
                return someTriggers.some((someTrigger) => {
                    const someIdentify = someTrigger.getSignature();

                    return identify === someIdentify;
                });
            }
        });

        if ( hasDuplicate ) {
            throw new Error(`duplicate trigger ${ identify }`);
        }
    }

    private parseFile(rootFolderPath: string, filePath: string): IFile | undefined {
        const sql = fs.readFileSync(filePath).toString();
        
        const sqlFile = this.fileParser.parse(sql);
        
        if ( !sqlFile ) {
            return;
        }

        const subPath = getSubPath(rootFolderPath, filePath);

        const fileName = filePath.split(/[/\\]/).pop() as string;

        return {
            name: fileName,
            folder: rootFolderPath,
            path: formatPath(subPath),
            content: sqlFile
        };
    }

    private onChangeWatcher(eventType: string, rootFolderPath: string, subPath: string) {
        // subPath path to file from rootFolderPath
        subPath = formatPath(subPath);
        // full path to file or dir with rootFolderPath
        const fullPath = rootFolderPath + "/" + subPath;


        if ( eventType === "remove" ) {
            this.onRemoveDirOrFile(rootFolderPath, subPath);
        }
        else {
            // ignore NOT sql files
            if ( !isSqlFile(fullPath) ) {
                return;
            }

            // if file was parsed early
            const parsedFile = this.files.find(file =>
                file.path === subPath &&
                file.folder === rootFolderPath
            );


            if ( parsedFile ) {
                this.onChangeFile(rootFolderPath, subPath, parsedFile);
            }
            else {
                this.onCreateFile(rootFolderPath, subPath);
            }
        }
    }

    private onRemoveDirOrFile(rootFolderPath: string, subPath: string) {
        let hasChange = false;

        const changes = Diff.empty();


        for (let i = 0, n = this.files.length; i < n; i++) {
            const file = this.files[ i ];

            if ( file.folder !== rootFolderPath ) {
                continue;
            }

            const isRemoved = (
                // removed this file
                file.path === subPath ||
                // removed dir, who contain this file
                file.path.startsWith( subPath + "/" )
            );

            if ( !isRemoved ) {
                continue;
            }


            // remove file, from state
            this.files.splice(i, 1);
            i--;
            n--;

            // generate event
            hasChange = true;

            changes.dropState(file.content);
        }
        

        if ( hasChange ) {
            this.emit("change", changes);
        }
    }

    private emitError(params: {subPath: string, err: Error}) {
        const outError = new Error(params.err.message) as any;
        
        outError.subPath = params.subPath;
        outError.originalError = params.err;

        this.emit("error", outError);
    }

    private onChangeFile(rootFolderPath: string, subPath: string, oldFile: IFile) {
        let newFile = null;

        try {
            newFile = this.parseFile(
                rootFolderPath,
                rootFolderPath + "/" + subPath
            );
        } catch(err) {
            this.emitError({
                subPath,
                err
            });
        }
        
        const hasChange = (
            JSON.stringify(newFile) 
            !==
            JSON.stringify(oldFile)
        );

        if ( !hasChange ) {
            return;
        }

        const fileIndex = this.files.indexOf( oldFile );
        this.files.splice(fileIndex, 1);

        const changes = Diff.empty()
            .dropState(oldFile.content);

        try {
            if ( newFile ) {
                this.checkDuplicate( newFile );

                changes.createState(newFile.content);

                this.files.splice(fileIndex, 0, newFile);
            }
        } catch(err) {
            this.emitError({
                subPath,
                err
            });
        }
        
        this.emit("change", changes);
    }

    private onCreateFile(rootFolderPath: string, subPath: string) {
        const file = this.parseFile(
            rootFolderPath,
            rootFolderPath + "/" + subPath
        );
        
        if ( !file ) {
            return;
        }

        this.checkDuplicate( file );

        this.files.push( file );
        
        const changes = Diff.empty().createState(file.content);

        this.emit("change", changes);
    }

    

}

function formatPath(inputFilePath: string) {
    const filePaths = inputFilePath.split(/[/\\]/);
    const outputFilePath = filePaths.join("/");

    return outputFilePath;
}

function isSqlFile(filePath: string) {
    if ( !/\.sql$/.test(filePath) ) {
        return false;
    }

    let stat;
    try {
        stat = fs.lstatSync(filePath);
    } catch(err) {
        return false;
    }

    return stat.isFile();
}

function getSubPath(rootFolderPath: string, fullFilePath: string) {
    const subPath = fullFilePath.slice( rootFolderPath.length + 1 );
    return subPath;
}
