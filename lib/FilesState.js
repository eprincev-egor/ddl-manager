"use strict";

const fs = require("fs");
const glob = require("glob");
const DDLCoach = require("./parser/DDLCoach");
const CreateFunction = require("./parser/syntax/CreateFunction");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const EventEmitter = require("events");

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
            let fileName = filePath.split(/[/\\]/).pop();
            let content = this.parseFile(filePath);

            if ( !content ) {
                return;
            }

            this.checkDuplicate( content );

            let subPath = filePath.slice( this.folder.length + 1 );

            this.files.push({
                name: fileName,
                path: formatPath(subPath),
                content
            });
        });
    }

    checkDuplicate(fileContent) {
        this.checkDuplicateFunction( fileContent.function );

        if ( fileContent.trigger ) {
            this.checkDuplicateTrigger( fileContent.trigger );
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
        let fileContent = fs.readFileSync(filePath);
        fileContent = fileContent.toString();

        let coach = new DDLCoach(fileContent);
        let sqlFile = coach.parseSqlFile();
        
        fileContent = sqlFile.toJSON();

        return fileContent;
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

    watch() {
        this.watcher = fs.watch(
            this.folder, 
            {recursive: true}, 
            this.onChangeFiles.bind(this)
        );
    }

    onChangeFiles(eventType, subPath) {
        // subPath path to file from this.folder
        subPath = formatPath(subPath);

        if ( eventType == "rename" ) {
            let fileIndex = this.files.findIndex(file => 
                file.path == subPath
            );

            let file = this.files[ fileIndex ];
            
            if ( !file ) {
                return;
            }

            this.files.splice(fileIndex, 1);

            this.emit("change", {
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
            });
        }
    }

    stopWatch() {
        if ( this.watcher ) {
            this.watcher.close();
            delete this.watcher;
        }
    }
}

function formatPath(filePath) {
    filePath = filePath.split(/[/\\]/);
    filePath = filePath.join("/");

    return filePath;
}

module.exports = FilesState;