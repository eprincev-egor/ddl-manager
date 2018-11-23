"use strict";

const fs = require("fs");
const glob = require("glob");
const DDLCoach = require("./parser/DDLCoach");

class FilesState {
    static create({folder}) {
        return new FilesState({
            folder
        });
    }

    constructor({folder}) {
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

            this.files.push({
                name: fileName,
                content
            });
        });
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
}

module.exports = FilesState;