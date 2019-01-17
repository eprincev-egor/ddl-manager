"use strict";

const fs = require("fs");
const glob = require("glob");
const DDLCoach = require("./parser/DDLCoach");
const CreateFunction = require("./parser/syntax/CreateFunction");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const EventEmitter = require("events");
const watch = require("node-watch");
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
        //        functions: [],
        //        triggers: [],
        //        comments: []
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

        content.functions.forEach(func => 
            this.checkDuplicateFunction( func )
        );

        if ( content.triggers ) {
            content.triggers.forEach(trigger => {
                this.checkDuplicateTrigger( trigger );
            });
        }
    }

    checkDuplicateFunction(func) {
        let identify = CreateFunction.function2identifySql(func);

        let hasDuplicate = this.files.some(someFile => {
            return someFile.content.functions.some(someFunc => {
                let someIdentify = CreateFunction.function2identifySql(someFunc);
                
                return identify == someIdentify;
            });
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
        let outFunctions = [];

        this.files.forEach(file => {
            file.content.functions.forEach(func => {
                outFunctions.push( func );
            });
        });

        return outFunctions;
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

    getComments() {
        let outComments = [];

        this.files.forEach(file => {
            let {comments} = file.content;

            if ( comments ) {
                outComments = outComments.concat( comments );
            }
        });

        return outComments;
    }

    getFiles() {
        return this.files;
    }

    async watch() {
        let handler = (eventType, subPath) => {
            try {
                this.onChangeWatcher(eventType, subPath);
            } catch(err) {
                this.emitError({
                    subPath,
                    err
                });
            }
        };

        return new Promise((resolve) => {
            this.fsWatcher = watch(this.folder, {
                recursive: true,
                delay: 5
            }, (eventType, fullPath) => {
                let subPath = path.relative(this.folder, fullPath);

                handler(eventType, subPath);
            });

            this.fsWatcher.on("ready", () => {
                resolve();
            });
        });
    }

    onChangeWatcher(eventType, subPath) {
        // subPath path to file from this.folder
        subPath = formatPath(subPath);
        // full path to file or dir with this.folder
        let fullPath = this.folder + "/" + subPath;


        if ( eventType == "remove" ) {
            this.onRemoveFile(subPath);
        }
        else {
            // ignore NOT sql files
            if ( !isSqlFile(fullPath) ) {
                return;
            }

            // if file was parsed early
            let parsedFile = this.files.find(file =>
                file.path == subPath
            );


            if ( parsedFile ) {
                this.onChangeFile(subPath, parsedFile);
            }
            else {
                this.onCreateFile(subPath);
            }
        }
    }

    onRemoveFile(subPath) {
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
                functions: file.content.functions,
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        };

        if ( file.content.triggers ) {
            changes.drop.triggers = file.content.triggers;
        }

        if ( file.content.comments ) {
            changes.drop.comments = file.content.comments;
        }

        this.emit("change", changes);
    }

    emitError({subPath, err}) {
        let outError = new Error(err.message);
        
        outError.subPath = subPath;
        outError.originalError = err;

        this.emit("error", outError);
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

        if ( oldFile.content.comments ) {
            changes.drop.comments = oldFile.content.comments;
        }

        try {
            if ( newFile ) {
                this.checkDuplicate( newFile );

                changes.create.functions = newFile.content.functions;
    
                if ( newFile.content.triggers ) {
                    changes.create.triggers = newFile.content.triggers;
                }

                if ( newFile.content.comments ) {
                    changes.create.comments = newFile.content.comments;
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
                functions: file.content.functions,
                triggers: []
            }
        };

        if ( file.content.triggers ) {
            changes.create.triggers = file.content.triggers;
        }

        if ( file.content.comments ) {
            changes.create.comments = file.content.comments;
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

function formatPath(filePath) {
    filePath = filePath.split(/[/\\]/);
    filePath = filePath.join("/");

    return filePath;
}

function isSqlFile(filePath) {
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

module.exports = FilesState;