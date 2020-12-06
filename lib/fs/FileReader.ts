import fs from "fs";
import glob from "glob";
import { EventEmitter } from "events";
import watch from "node-watch";
import path from "path";
import { FileParser } from "../parser";
import { File } from "./File";
import { Cache } from "../ast";
import { FSEvent } from "./FSEvent";
import { FilesState } from "./FilesState";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";

export class FileReader extends EventEmitter {
    static read(params: {folder: string | string[], onError?: any}) {

        const reader = new FileReader({
            folder: params.folder
        });

        if ( params.onError ) {
            reader.on("error", params.onError);
        }

        reader.parse();

        return reader;
    }

    readonly state: FilesState;
    readonly rootFolders: string[];
    private fsWatcher?: ReturnType<typeof watch>;

    private fileParser: FileParser;
    
    constructor(params: {folder: string | string[]}) {
        super();

        this.fileParser = new FileParser();
        this.state = new FilesState();

        if ( typeof params.folder === "string" ) {
            this.rootFolders = [params.folder];
        }
        else {
            this.rootFolders = params.folder;
        }
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

        const promise = Promise.all(this.rootFolders.map(rootFolder =>
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
        for (const folderPath of this.rootFolders) {
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

            let file: File | undefined;
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
                this.state.addFile(file);
            }
        });
    }

    private checkDuplicate(file: File) {
        file.content.functions.forEach(func => 
            this.checkDuplicateFunction( func )
        );
        file.content.triggers.forEach(trigger => {
            this.checkDuplicateTrigger( trigger );
        });
        file.content.cache.forEach(cache => {
            this.checkDuplicateCache( cache );
        });
    }

    private checkDuplicateFunction(func: DatabaseFunction) {
        const identify = func.getSignature();

        const hasDuplicate = this.state.files.some(someFile => {
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

        const hasDuplicate = this.state.files.some(someFile => {
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

    private checkDuplicateCache(cache: Cache) {
        const identify = cache.getSignature();
        const cacheColumns = cache.select.columns.map(col => col.name);;

        for (const someFile of this.state.files) {
            for (const someCache of someFile.content.cache) {
                // duplicated cache name
                if ( someCache.getSignature() === identify ) {
                    throw new Error(`duplicate ${ identify }`);
                }
                // duplicate cache columns

                if ( someCache.for.table.equal(cache.for.table) ) {
                    const someCacheColumns = someCache.select.columns.map(col => col.name);
                    const duplicatedColumns = someCacheColumns.filter(columnName =>
                        cacheColumns.includes(columnName)
                    );

                    throw new Error(`duplicated columns: ${ duplicatedColumns } by cache: ${cache.name}, ${someCache.name}`);
                }
            }
        }
    }

    private parseFile(rootFolderPath: string, filePath: string): File | undefined {
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
            const parsedFile = this.state.files.find(file =>
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

        let fsEvent = new FSEvent();


        for (let i = 0, n = this.state.files.length; i < n; i++) {
            const file = this.state.files[ i ];

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
            this.state.removeFile(file);
            i--;
            n--;

            // generate event
            hasChange = true;

            fsEvent = fsEvent.remove(file);
        }
        

        if ( hasChange ) {
            this.emit("change", fsEvent);
        }
    }

    private emitError(params: {subPath: string, err: Error}) {
        const outError = new Error(params.err.message) as any;
        
        outError.subPath = params.subPath;
        outError.originalError = params.err;

        this.emit("error", outError);
    }

    private onChangeFile(rootFolderPath: string, subPath: string, oldFile: File) {
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

        this.state.removeFile(oldFile);

        let fsEvent = new FSEvent({
            removed: [oldFile]
        });

        try {
            if ( newFile ) {
                this.checkDuplicate( newFile );

                fsEvent = fsEvent.create(newFile);

                this.state.addFile(newFile);
            }
        } catch(err) {
            this.emitError({
                subPath,
                err
            });
        }
        
        this.emit("change", fsEvent);
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

        this.state.addFile( file );
        
        const fsEvent = new FSEvent({created: [file]});

        this.emit("change", fsEvent);
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
