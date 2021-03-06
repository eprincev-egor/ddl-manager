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

        let folders = folder;
        if ( typeof folder === "string" ) {
            folders = [folder];
        }

        this.folders = folders;
        this.files = [];
    }

    parse() {
        for (const folderPath of this.folders) {
            this.parseFolder(folderPath);
        }
    }

    parseFolder(folderPath) {

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
                file = this.parseFile(folderPath, filePath);

                if ( file && file.content ) {
                    this.checkDuplicate( file );
                }
            } catch(err) {
                this.emitError({
                    rootFolderPath: folderPath,
                    subPath: filePath.slice( folderPath.length + 1 ),
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

    parseFile(rootFolderPath, filePath) {
        let sql = fs.readFileSync(filePath).toString();
        
        let coach = new DDLCoach(sql);
        coach.replaceComments();
        
        if ( coach.str.trim() === "" ) {
            return;
        }

        let sqlFile = coach.parseSqlFile();
        
        let subPath = getSubPath(rootFolderPath, filePath);

        let fileName = filePath.split(/[/\\]/).pop();

        return {
            name: fileName,
            folder: rootFolderPath,
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
        let handler = (eventType, rootFolder, subPath) => {
            try {
                this.onChangeWatcher(eventType, rootFolder, subPath);
            } catch(err) {
                this.emitError({
                    rootFolderPath: rootFolder,
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
                    let subPath = path.relative(rootFolder, fullPath);
    
                    handler(eventType, rootFolder, subPath);
                });
    
                this.fsWatcher.on("ready", () => {
                    resolve();
                });
            })
        ));

        return promise;
    }

    onChangeWatcher(eventType, rootFolderPath, subPath) {
        // subPath path to file from rootFolderPath
        subPath = formatPath(subPath);
        // full path to file or dir with rootFolderPath
        let fullPath = rootFolderPath + "/" + subPath;


        if ( eventType == "remove" ) {
            this.onRemoveDirOrFile(rootFolderPath, subPath);
        }
        else {
            // ignore NOT sql files
            if ( !isSqlFile(fullPath) ) {
                return;
            }

            // if file was parsed early
            let parsedFile = this.files.find(file =>
                file.path == subPath &&
                file.folder == rootFolderPath
            );


            if ( parsedFile ) {
                this.onChangeFile(rootFolderPath, subPath, parsedFile);
            }
            else {
                this.onCreateFile(rootFolderPath, subPath);
            }
        }
    }

    onRemoveDirOrFile(rootFolderPath, subPath) {
        let hasChange = false;
        let changes = {
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
            let file = this.files[ i ];

            if ( file.folder != rootFolderPath ) {
                continue;
            }

            let isRemoved = (
                // removed this file
                file.path == subPath ||
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
    
            // or comment for object (func/trigger)
            if ( file.content.comments ) {
                changes.drop.comments = file.content.comments;
            }
        }
        

        if ( hasChange ) {
            this.emit("change", changes);
        }
    }

    emitError({subPath, err}) {
        let outError = new Error(err.message);
        
        outError.subPath = subPath;
        outError.originalError = err;

        this.emit("error", outError);
    }

    onChangeFile(rootFolderPath, subPath, oldFile) {
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
                rootFolderPath,
                subPath,
                err
            });
        }
        
        this.emit("change", changes);
    }

    onCreateFile(rootFolderPath, subPath) {
        let file = this.parseFile(
            rootFolderPath,
            rootFolderPath + "/" + subPath
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

function getSubPath(rootFolderPath, fullFilePath) {
    let subPath = fullFilePath.slice( rootFolderPath.length + 1 );
    return subPath;
}

module.exports = FilesState;