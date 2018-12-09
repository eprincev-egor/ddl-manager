"use strict";

const fs = require("fs");
const glob = require("glob");
const DDLCoach = require("./parser/DDLCoach");
const CreateFunction = require("./parser/syntax/CreateFunction");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const EventEmitter = require("events");
const watch = require("watch");
const path = require("path");

class FilesState extends EventEmitter {
    static create({folder, onError}) {
        let filesState = new FilesState({
            folder
        });

        if ( onError ) {
            filesState.on("error", onError);
        }

        filesState.parse();

        return filesState;
    }

    constructor({folder}) {
        super();

        this.folder = folder;
        this.files = [];
    }

    parse() {
        let folderPath = this.folder;

        if ( !fs.existsSync(folderPath) ) {
            throw new Error(`folder "${ folderPath }" not found`);
        }

        // fill this.files, make array of object:
        // {
        //   name: "some-file-name.sql",
        //   path: "/path/to/some-file-name.sql",
        //   content: {
        //        function: ...
        //   }
        // }

        let files = glob.sync(folderPath + "/**/*.sql");
        files.forEach(filePath => {
            // ignore dirs with *.sql name
            //   ./dir.sql/file
            let stat = fs.lstatSync(filePath);
            if ( !stat.isFile() ) {
                return;
            }

            let file;
            try {
                file = this.parseFile(filePath);

                if ( file && file.content ) {
                    this.checkDuplicate( file );
                }
            } catch(err) {
                this.emitError({
                    subPath: filePath.slice( this.folder.length + 1 ),
                    err
                });
            }
            
            if ( file ) {
                this.files.push(file);
            }
        });
    }

    checkDuplicate(file) {
        let content = file.content;

        this.checkDuplicateFunction( content.function );

        if ( content.triggers ) {
            content.triggers.forEach(trigger => {
                this.checkDuplicateTrigger( trigger );
            });
        }
    }

    checkDuplicateFunction(func) {
        let identify = CreateFunction.function2identifySql(func);

        let hasDuplicate = this.files.some(someFile => {
            let someFunc = someFile.content.function;
            let someIdentify = CreateFunction.function2identifySql(someFunc);
            
            return identify == someIdentify;
        });

        if ( hasDuplicate ) {
            throw new Error(`duplicate function ${ identify }`);
        }
    }

    checkDuplicateTrigger(trigger) {
        let identify = CreateTrigger.trigger2identifySql( trigger );

        let hasDuplicate = this.files.some(someFile => {
            let someTriggers = someFile.content.triggers;

            if ( someTriggers ) {
                return someTriggers.some(someTrigger => {
                    let someIdentify = CreateTrigger.trigger2identifySql( someTrigger );

                    return identify == someIdentify;
                });
            }
        });

        if ( hasDuplicate ) {
            throw new Error(`duplicate trigger ${ identify }`);
        }
    }

    parseFile(filePath) {
        let sql = fs.readFileSync(filePath).toString();
        
        let coach = new DDLCoach(sql);
        coach.replaceComments();
        
        if ( coach.str.trim() === "" ) {
            return;
        }

        let sqlFile = coach.parseSqlFile();
        
        let subPath = filePath.slice( this.folder.length + 1 );
        let fileName = filePath.split(/[/\\]/).pop();

        return {
            name: fileName,
            path: formatPath(subPath),
            content: sqlFile.toJSON()
        };
    }

    getFunctions() {
        return this.files.map(file => 
            file.content.function
        );
    }

    getTriggers() {
        let outTriggers = [];

        this.files.forEach(file => {
            let {triggers} = file.content;
            
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
        return new Promise((resolve) => {
            let handler = (subPath) => {
                try {
                    this.onChangeWatcher(subPath);
                } catch(err) {
                    this.emitError({
                        subPath,
                        err
                    });
                }
            };

            watch.createMonitor(path.resolve(this.folder), {
                interval: 0.001
            }, (watcher) => {
                // for linux recursive
                this.watcherMonitor = watcher;
                
                this.watcherMonitor.on("created", (filePath) => {
                    let subPath = filePath.slice( this.folder.length + 1 );
                    handler(subPath);
                });
                this.watcherMonitor.on("changed", (filePath) => {
                    let subPath = filePath.slice( this.folder.length + 1 );
                    handler(subPath);
                });
                this.watcherMonitor.on("removed", (filePath) => {
                    let subPath = filePath.slice( this.folder.length + 1 );
                    handler(subPath);
                });

                // for circle ci
                this.fsWatcher = fs.watch(this.folder, {
                    recursive: true
                }, (eventType, subPath) => {
                    handler(subPath);
                });

                resolve();
            });
        });
    }

    onChangeWatcher(subPath) {
        // subPath path to file from this.folder
        subPath = formatPath(subPath);

        if ( !/\.sql$/.test(subPath) ) {
            return;
        }

        let filePath = this.folder + "/" + subPath;

        let exists = false;
        let stat;
        try {
            stat = fs.lstatSync(filePath);
            exists = true;
        } catch(err) {
            exists = false;
        }

        // ignore dirs with *.sql name
        //   ./dir.sql/file
        if ( stat && !stat.isFile() ) {
            return;
        }
        

        if ( exists ) {
            let fileIndex = this.files.findIndex(file => 
                file.path == subPath
            );
            let file = this.files[ fileIndex ];
    
            if ( !file ) {
                this.onCreateFile(subPath);
            } else {
                this.onChangeFile(subPath, file);
            }
        }
        else {
            this.onRemove(subPath);
        }
    }

    onRemove(subPath) {
        let fileIndex = this.files.findIndex(file => 
            file.path == subPath
        );

        let file = this.files[ fileIndex ];

        if ( !file ) {
            return;
        }

        this.files.splice(fileIndex, 1);

        let changes = {
            drop: {
                functions: [
                    file.content.function
                ],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        };

        if ( file.content.triggers ) {
            changes.drop.triggers = changes.drop.triggers
                .concat(
                    file.content.triggers
                );
        }

        this.emit("change", changes);
    }

    emitError({subPath, err}) {
        this.emit("error", {
            subPath,
            message: err.message,
            originalError: err
        });
    }

    onChangeFile(subPath, oldFile) {
        let newFile = null;

        try {
            newFile = this.parseFile(
                this.folder + "/" + subPath
            );
        } catch(err) {
            this.emitError({
                subPath,
                err
            });
        }
        
        let hasChange = (
            JSON.stringify(newFile) 
            !=
            JSON.stringify(oldFile)
        );

        if ( !hasChange ) {
            return;
        }

        let fileIndex = this.files.indexOf( oldFile );
        this.files.splice(fileIndex, 1);

        let changes = {
            drop: {
                functions: [
                    oldFile.content.function
                ],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        };

        if ( oldFile.content.triggers ) {
            changes.drop.triggers = changes.drop.triggers
                .concat(
                    oldFile.content.triggers
                );
        }

        try {
            if ( newFile ) {
                this.checkDuplicate( newFile );

                changes.create.functions.push(
                    newFile.content.function
                );
    
                if ( newFile.content.triggers ) {
                    changes.create.triggers = changes.create.triggers
                        .concat(
                            newFile.content.triggers
                        );
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

    onCreateFile(subPath) {
        let file = this.parseFile(
            this.folder + "/" + subPath
        );
        
        if ( !file ) {
            return;
        }

        this.checkDuplicate( file );

        this.files.push( file );
        
        let changes = {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    file.content.function
                ],
                triggers: []
            }
        };

        if ( file.content.triggers ) {
            changes.create.triggers = changes.create.triggers
                .concat(
                    file.content.triggers
                );
        }

        this.emit("change", changes);
    }

    stopWatch() {
        if ( this.watcherMonitor ) {
            this.watcherMonitor.stop();
            delete this.watcherMonitor;
        }

        if ( this.fsWatcher ) {
            this.fsWatcher.close();
            delete this.fsWatcher;
        }
    }
}

function formatPath(filePath) {
    filePath = filePath.split(/[/\\]/);
    filePath = filePath.join("/");

    return filePath;
}


module.exports = FilesState;