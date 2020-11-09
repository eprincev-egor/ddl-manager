import fs from "fs";
import glob from "glob";
import { EventEmitter } from "events";
import watch from "node-watch";
import path from "path";
import { FileParser } from "./parser/FileParser";
import { IDiff, IFile } from "./interface";
import { DatabaseTrigger } from "./ast/DatabaseTrigger";
import { DatabaseFunction } from "./ast/DatabaseFunction";

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

    folders: string[];
    files: IFile[];
    fsWatcher?: ReturnType<typeof watch>;

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

    parse() {
        for (const folderPath of this.folders) {
            this.parseFolder(folderPath);
        }
    }

    parseFolder(folderPath: string) {

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

    checkDuplicate(file: IFile) {
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

    checkDuplicateFunction(func: DatabaseFunction) {
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

    checkDuplicateTrigger(trigger: DatabaseTrigger) {
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

    parseFile(rootFolderPath: string, filePath: string): IFile | undefined {
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

    getFunctions() {
        const outFunctions: DatabaseFunction[] = [];

        this.files.forEach((file) => {
            file.content.functions.forEach((func) => {
                outFunctions.push( func );
            });
        });

        return outFunctions;
    }

    getTriggers() {
        let outTriggers: DatabaseTrigger[] = [];

        this.files.forEach(file => {
            const {triggers} = file.content;
            
            if ( triggers ) {
                outTriggers = outTriggers.concat(triggers);
            }
        });

        return outTriggers;
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

    onChangeWatcher(eventType: string, rootFolderPath: string, subPath: string) {
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

    onRemoveDirOrFile(rootFolderPath: string, subPath: string) {
        let hasChange = false;
        const changes: IDiff = {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        };


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

            // any file has functions
            changes.drop.functions = changes.drop.functions.concat(
                file.content.functions
            );

            // sometimes file can contain trigger
            if ( file.content.triggers ) {
                changes.drop.triggers = file.content.triggers;
            }
        }
        

        if ( hasChange ) {
            this.emit("change", changes);
        }
    }

    emitError(params: {subPath: string, err: Error}) {
        const outError = new Error(params.err.message) as any;
        
        outError.subPath = params.subPath;
        outError.originalError = params.err;

        this.emit("error", outError);
    }

    onChangeFile(rootFolderPath: string, subPath: string, oldFile: IFile) {
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

        const changes: IDiff = {
            drop: {
                functions: oldFile.content.functions,
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        };

        if ( oldFile.content.triggers ) {
            changes.drop.triggers = oldFile.content.triggers;
        }

        try {
            if ( newFile ) {
                this.checkDuplicate( newFile );

                changes.create.functions = newFile.content.functions;
    
                if ( newFile.content.triggers ) {
                    changes.create.triggers = newFile.content.triggers;
                }

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

    onCreateFile(rootFolderPath: string, subPath: string) {
        const file = this.parseFile(
            rootFolderPath,
            rootFolderPath + "/" + subPath
        );
        
        if ( !file ) {
            return;
        }

        this.checkDuplicate( file );

        this.files.push( file );
        
        const changes: IDiff = {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: file.content.functions,
                triggers: []
            }
        };

        if ( file.content.triggers ) {
            changes.create.triggers = file.content.triggers;
        }

        this.emit("change", changes);
    }

    stopWatch() {
        if ( this.fsWatcher ) {
            this.fsWatcher.close();
            delete this.fsWatcher;
        }
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
