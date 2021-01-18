import { EventEmitter } from "events";
import watch from "node-watch";
import path from "path";
import { File } from "./File";
import { FSEvent } from "./FSEvent";
import { FilesState } from "./FilesState";
import { FileReader } from "./FileReader";
import { isSqlFile, formatPath, prepareError } from "./utils";

export class FileWatcher extends EventEmitter {

    static async watch(rootFolders: string[]) {
        const watcher = new FileWatcher(rootFolders);
        await watcher.watch();
        return watcher;
    }

    readonly state: FilesState;
    private rootFolders: string[];
    private fsWatcher?: ReturnType<typeof watch>;
    private reader: FileReader;

    private constructor(rootFolders: string[]) {
        super();
        this.reader = new FileReader(rootFolders);
        this.state = this.reader.state;
        this.rootFolders = rootFolders;
    }

    async watch() {
        this.reader.read();

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
            new Promise<void>((resolve) => {
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

    private onChangeFile(rootFolderPath: string, subPath: string, oldFile: File) {
        let newFile = null;

        try {
            newFile = this.reader.parseFile(
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
                this.state.addFile(newFile);
                fsEvent = fsEvent.create(newFile);
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
        const file = this.reader.parseFile(
            rootFolderPath,
            rootFolderPath + "/" + subPath
        );
        
        if ( !file ) {
            return;
        }

        this.state.addFile( file );
        
        const fsEvent = new FSEvent({created: [file]});

        this.emit("change", fsEvent);
    }

    private emitError(params: {subPath: string, err: Error}) {
        const outError = prepareError(params.err, params.subPath);
        this.emit("error", outError);
    }
}
