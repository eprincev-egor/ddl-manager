import FSDriver, {IDirContent} from "../../lib/fs/FSDriver";

export interface IFiles {
    [key: string]: string;
}

export default class TestFSDriver extends FSDriver {
    files: IFiles;
    dirContentByPath: {[filePath: string]: IDirContent};

    constructor(files: IFiles) {
        super();
        this.files = files;

        this.dirContentByPath = {};

        // filePath: "path/to/some/file.sql"
        for (const filePath in files) {
            // dirNames: ["path", "to", "some"]
            const dirNames = filePath.split("/").slice(0, 1);
            const fileName = filePath.split("/").pop();

            let folderPath = "";
            let lastDirContent: IDirContent;
            for (const dirName of dirNames) {
                folderPath = folderPath + "/" + dirName;

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
    }

    async readFile(filePath: string): Promise<string> {
        return this.files[ filePath ];
    }

    async readFolder(folderPath: string): Promise<IDirContent> {
        return this.dirContentByPath[ folderPath ];
    }
}