import FSDriver, {IDirectory} from "../../lib/fs/FSDriver";
import {TestFSDirectory} from "./TestFSDirectory";

export interface IFiles {
    [key: string]: string;
}

export default class TestFSDriver extends FSDriver {
    private files: IFiles;
    private dirContentByPath: {[filePath: string]: TestFSDirectory};

    constructor(files: IFiles) {
        super();
        this.files = {};

        this.dirContentByPath = {
            ".": new TestFSDirectory()
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

        let lastDirContent: TestFSDirectory;
        for (let i = 0, n = dirNames.length; i < n; i++) {
            const dirName = dirNames[i];
            const folderPath = dirNames.slice(0, i + 1).join("/");

            const directory = this.getOrCreateDirectory( folderPath );

            if ( lastDirContent ) {
                lastDirContent.addDirectory(dirName);
            }

            lastDirContent = directory;
        }

        lastDirContent.addFile(fileName);
    }

    getFile(filePath: string): string {
        return this.files[ filePath ];
    }

    private getDirectory(directoryPath: string): TestFSDirectory {
        return this.dirContentByPath[ directoryPath ];
    }

    private getOrCreateDirectory(directoryPath: string): TestFSDirectory {
        const existentDirectory = this.getDirectory( directoryPath );
        if ( existentDirectory ) {
            return existentDirectory;
        }

        const newDirectory = new TestFSDirectory();
        this.dirContentByPath[ directoryPath ] = newDirectory;
        
        return newDirectory;
    }

    removeTestFile(filePath: string) {
        // dirNames: [".", "path", "to", "some"]
        const dirNames = filePath.split("/").slice(0, -1);
        // fileName: "file.sql"
        const fileName = filePath.split("/").pop();

        for (let i = dirNames.length - 1; i >= 0; i--) {
            const dirName = dirNames[i];
            const folderPath = dirNames.slice(0, i + 1).join("/");

            const directory = this.getDirectory(folderPath);
            if ( !directory ) {
                continue;
            }

            directory.removeFile(fileName);

            if ( directory.isEmpty() ) {
                delete this.dirContentByPath[ folderPath ];

                if ( i > 0 ) {
                    const parentDirectoryPath = dirNames.slice(0, i).join("/");
                    const parentDirectory = this.getDirectory(parentDirectoryPath);
                    parentDirectory.removeDirectory( dirName );
                }
            }
        }
    }

    async readFile(filePath: string): Promise<string> {
        return this.files[ filePath ];
    }

    async readFolder(folderPath: string): Promise<IDirectory> {
        return this.getDirectory( folderPath );
    }
}