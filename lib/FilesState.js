"use strict";

const fs = require("fs");
const glob = require("glob");
const DDLCoach = require("./parser/DDLCoach");
const CreateFunction = require("./parser/syntax/CreateFunction");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const EventEmitter = require("events");
const chokidar = require("chokidar");

class FilesState extends EventEmitter {
    static create({folder}) {
        return new FilesState({
            folder
        });
    }

    constructor({folder}) {
        super();

        this.folder = folder;

        this.files = [];
        
        this.parse();
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
            let file = this.parseFile(filePath);
            
            if ( !file.content ) {
                return;
            }

            this.checkDuplicate( file );

            this.files.push(file);
        });
    }

    checkDuplicate(file) {
        let content = file.content;

        this.checkDuplicateFunction( content.function );

        if ( content.trigger ) {
            this.checkDuplicateTrigger( content.trigger );
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
            let someTrigger = someFile.content.trigger;

            if ( someTrigger ) {
                let someIdentify = CreateTrigger.trigger2identifySql( someTrigger );
            
                return identify == someIdentify;
            }
        });

        if ( hasDuplicate ) {
            throw new Error(`duplicate trigger ${ identify }`);
        }
    }

    parseFile(filePath) {
        let sql = fs.readFileSync(filePath).toString();

        let coach = new DDLCoach(sql);
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
        let triggers = [];

        this.files.forEach(file => {
            let {trigger} = file.content;
            
            if ( trigger ) {
                triggers.push( trigger );
            }
        });

        return triggers;
    }

    getFiles() {
        return this.files;
    }

    async watch() {
        this.chokidarWatcher = chokidar.watch(".", {
            cwd: this.folder
        });

        this.fsWatcher = fs.watch(this.folder, {
            recursive: true
        }, (eventType, subPath) => {
            this.onChangeWatcher(subPath);
        });

        this.chokidarWatcher.on("all", (eventType, subPath) => {
            this.onChangeWatcher(subPath);
        });
    }

    onChangeWatcher(subPath) {
        // subPath path to file from this.folder
        subPath = formatPath(subPath);

        if ( !/\.sql$/.test(subPath) ) {
            return;
        }

        let filePath = this.folder + "/" + subPath;

        if ( fs.existsSync( filePath ) ) {
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

        if ( file.content.trigger ) {
            changes.drop.triggers.push(
                file.content.trigger
            );
        }

        this.emit("change", changes);
    }

    onChangeFile(subPath, oldFile) {
        let newFile = this.parseFile(
            this.folder + "/" + subPath
        );

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

        try {
            this.checkDuplicate( newFile );
        } catch(err) {
            this.emit("error", err);
            return;
        }

        this.files.splice(fileIndex, 0, newFile);

        let changes = {
            drop: {
                functions: [
                    oldFile.content.function
                ],
                triggers: []
            },
            create: {
                functions: [
                    newFile.content.function
                ],
                triggers: []
            }
        };

        if ( oldFile.content.trigger ) {
            changes.drop.triggers.push(
                oldFile.content.trigger
            );
        }
        if ( newFile.content.trigger ) {
            changes.create.triggers.push(
                newFile.content.trigger
            );
        }

        this.emit("change", changes);
    }

    onCreateFile(subPath) {
        let file = this.parseFile(
            this.folder + "/" + subPath
        );

        try {
            this.checkDuplicate( file );
        } catch(err) {
            this.emit("error", err);
            return;
        }

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

        if ( file.content.trigger ) {
            changes.create.triggers.push(
                file.content.trigger
            );
        }

        this.emit("change", changes);
    }

    stopWatch() {
        if ( this.fsWatcher ) {
            this.fsWatcher.close();
            delete this.fsWatcher;
        }

        if ( this.chokidarWatcher ) {
            this.chokidarWatcher.close();
            delete this.chokidarWatcher;
        }
    }
}

function formatPath(filePath) {
    filePath = filePath.split(/[/\\]/);
    filePath = filePath.join("/");

    return filePath;
}


module.exports = FilesState;