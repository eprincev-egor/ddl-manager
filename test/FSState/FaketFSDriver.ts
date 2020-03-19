import FSDriver, {IDirectory} from "../../lib/fs/FSDriver";
import {TestFSDirectory} from "./FakeFSDirectory";

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
            "": new TestFSDirectory()
        };

        // filePath: "./path/to/some/file.sql"
        for (const filePath in files) {
            const fileContent = files[ filePath ];
            this.setTestFile(filePath, fileContent);
        }
    }

    setTestFile(filePath: string, fileContent: string) {
        filePath = normalizeFilePath(filePath);
        
        // delete directories structure, if file exists
        this.removeTestFile( filePath );

        this.files[ filePath ] = fileContent;
        
        // dirNames: ["path", "to", "some"]
        const dirNames = filePath.split("/").slice(0, -1);

        // fileName: "file.sql"
        const fileName = filePath.split("/").pop();

        let lastDirContent = this.getDirectory("");
        for (let i = 0, n = dirNames.length; i < n; i++) {
            const dirName = dirNames[i];
            const folderPath = dirNames.slice(0, i + 1).join("/");

            const directory = this.getOrCreateDirectory( folderPath );

            lastDirContent.addDirectory(dirName);
            lastDirContent = directory;
        }

        lastDirContent.addFile(fileName);
    }

    getFile(filePath: string): string {
        return this.files[ filePath ];
    }

    private getDirectory(directoryPath: string): TestFSDirectory {
        directoryPath = normalizeDirPath(directoryPath);
        return this.dirContentByPath[ directoryPath ];
    }

    private getOrCreateDirectory(directoryPath: string): TestFSDirectory {
        directoryPath = normalizeDirPath(directoryPath);

        const existentDirectory = this.getDirectory( directoryPath );
        if ( existentDirectory ) {
            return existentDirectory;
        }

        const newDirectory = new TestFSDirectory();
        this.dirContentByPath[ directoryPath ] = newDirectory;
        
        return newDirectory;
    }

    removeTestFile(filePath: string) {
        filePath = normalizeFilePath(filePath);

        delete this.files[ filePath ];

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
        filePath = normalizeFilePath(filePath);

        return this.files[ filePath ];
    }

    async readFolder(folderPath: string): Promise<IDirectory> {
        return this.getDirectory( folderPath );
    }

    async existsFile(filePath: string): Promise<boolean> {
        return filePath in this.files;
    }
}

function normalizeDirPath(dirPath: string): string {
    dirPath = dirPath.replace(/^\.\//, "");
    dirPath = dirPath.replace(/\\/g, "/");
    dirPath = dirPath.replace(/\/$/, "");
    return dirPath;
}

function normalizeFilePath(filePath: string): string {
    filePath = normalizeDirPath(filePath);
    return filePath;
}