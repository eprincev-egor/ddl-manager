import FSDriver, {IDirContent} from "../../lib/fs/FSDriver";

export interface IFiles {
    [key: string]: string;
}

export default class TestFSDriver extends FSDriver {
    files: IFiles;
    dirContentByPath: {[filePath: string]: IDirContent};

    constructor(files: IFiles) {
        super();
        this.files = {};

        this.dirContentByPath = {
            ".": {
                files: [],
                folders: []
            }
        };

        // filePath: "./path/to/some/file.sql"
        for (const filePath in files) {
            const fileContent = files[ filePath ];
            this.setTestFile(filePath, fileContent);
        }
    }

    setTestFile(filePath: string, fileContent: string) {
        this.files[ filePath ] = fileContent;
        
        // delete directories structure, if file
        this.removeTestFile( filePath );

        // dirNames: [".", "path", "to", "some"]
        const dirNames = filePath.split("/").slice(0, -1);
        // fileName: "file.sql"
        const fileName = filePath.split("/").pop();

        let lastDirContent: IDirContent;
        for (let i = 0, n = dirNames.length; i < n; i++) {
            const dirName = dirNames[i];
            const folderPath = dirNames.slice(0, i + 1).join("/");

            let dirContent = this.dirContentByPath[ folderPath ];
            if ( !dirContent ) {
                dirContent = {
                    files: [],
                    folders: []
                };

                this.dirContentByPath[ folderPath ] = dirContent;
            }

            if ( lastDirContent ) {
                lastDirContent.folders.push( dirName );
            }

            lastDirContent = dirContent;
        }

        lastDirContent.files.push(fileName);
    }

    removeTestFile(filePath: string) {
        // dirNames: [".", "path", "to", "some"]
        const dirNames = filePath.split("/").slice(0, -1);
        // fileName: "file.sql"
        const fileName = filePath.split("/").pop();

        for (let i = dirNames.length - 1; i >= 0; i--) {
            const dirName = dirNames[i];
            const folderPath = dirNames.slice(0, i + 1).join("/");

            const dirContent = this.dirContentByPath[ folderPath ];
            if ( !dirContent ) {
                continue;
            }

            const fileIndexInsideDirectory = dirContent.files.indexOf(fileName);
            if ( fileIndexInsideDirectory !== -1 ) {
                dirContent.files.splice(fileIndexInsideDirectory, 1);
            }

            const isEmptyDirectory = (
                dirContent.files.length === 0 &&
                dirContent.folders.length === 0
            );
            if ( isEmptyDirectory ) {
                delete this.dirContentByPath[ folderPath ];

                if ( i > 0 ) {
                    const parentDirectoryPath = dirNames.slice(0, i).join("/");
                    const parentDirectoryContent = this.dirContentByPath[ parentDirectoryPath ];
                    const currentDirectoryIndexInsideParentDirectory = parentDirectoryContent.folders.indexOf( dirName );
                    if ( currentDirectoryIndexInsideParentDirectory !== -1 ) {
                        parentDirectoryContent.folders.splice(currentDirectoryIndexInsideParentDirectory, 1);
                    }
                }
            }
        }
    }

    async readFile(filePath: string): Promise<string> {
        return this.files[ filePath ];
    }

    async readFolder(folderPath: string): Promise<IDirContent> {
        return this.dirContentByPath[ folderPath ];
    }
}