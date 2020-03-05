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
            this.addTestFile(filePath, fileContent);
        }
    }

    addTestFile(filePath: string, fileContent: string) {
        this.files[ filePath ] = fileContent;

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

    async readFile(filePath: string): Promise<string> {
        return this.files[ filePath ];
    }

    async readFolder(folderPath: string): Promise<IDirContent> {
        return this.dirContentByPath[ folderPath ];
    }
}