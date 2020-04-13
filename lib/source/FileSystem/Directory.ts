import { File } from "./File";

export interface IDirectoryParams {
    name: string;
    path: string;
    files: File[];
    directories: Directory[];
}

export class Directory {
    name: string;
    path: string;
    files: File[];
    directories: Directory[];

    constructor(params: IDirectoryParams) {
        this.name = params.name;
        this.path = params.path;
        this.files = params.files;
        this.directories = params.directories;
    }

    getFileByPath(filePath: string) {
        return this.findFile(file => 
            file.path === filePath
        );
    }

    getFileBySQL(sql: string) {
        return this.findFile(file => 
            file.sql === sql
        );
    }

    deepRemoveFile(filePath: string) {
        const {file, directory} = this.findFileAndDirectory((someFile) => 
            someFile.path === filePath
        );

        if ( directory ) {
            directory.removeFile(file)
        }
    }

    deepAddFile(file: File) {
        const {directory} = this.findFileAndDirectory((someFile) => 
            someFile.path === file.path
        );

        if ( directory ) {
            directory.addFile(file);
        }
    }

    addFile(file: File) {
        this.files.push(file);
    }

    removeFile(file: File) {
        const fileIndex = this.files.indexOf(file);
        if ( fileIndex !== -1 ) {
            this.files.splice(fileIndex, 1);
        }
    }

    private findFile(filterFunc: (file: File) => boolean): File {
        const result = this.findFileAndDirectory(filterFunc);
        if ( result ) {
            return result.file;
        }
    }

    private findFileAndDirectory(filterFunc: (file: File) => boolean): {
        file: File,
        directory: Directory
    } {
        let directory: Directory;
        let file: File;

        this.walk((nextDirectory) => {
            const fileInDirectory = nextDirectory.files.find(file => 
                filterFunc(file)
            );

            if ( fileInDirectory ) {
                directory = nextDirectory;
                file = fileInDirectory;
                return false;
            }
        });

        return {
            file,
            directory
        };
    }

    private walk(iteration: (directory: Directory) => boolean, startFromDirectory = this) {
        let needContinue: boolean;

        needContinue = iteration(this);
        if ( needContinue === false ) {
            return false;
        }

        for (const subDirectory of this.directories) {
            needContinue = subDirectory.walk(iteration);
            if ( needContinue === false ) {
                return false;
            }
        }
    }
}