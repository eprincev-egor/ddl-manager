import {IDirectory} from "../../lib/fs/FSDriver";

export class TestFSDirectory
implements IDirectory
{
    directories: string[];
    files: string[];

    constructor(directory: IDirectory = {directories: [], files: []}) {
        this.directories = directory.directories;
        this.files = directory.files;
    }

    addFile(fileName: string) {
        this.files.push(fileName);
    }

    addDirectory(dirName: string) {
        this.directories.push(dirName);
    }

    removeFile(fileName: string) {
        const fileIndex = this.files.indexOf(fileName);
        if ( fileIndex !== -1 ) {
            this.files.splice(fileIndex);
        }
    }

    removeDirectory(dirName: string) {
        const directoryIndex = this.files.indexOf(dirName);
        if ( directoryIndex !== -1 ) {
            this.files.splice(directoryIndex);
        }
    }

    isEmpty() {
        return (
            this.directories.length === 0 &&
            this.files.length === 0
        );
    }
}