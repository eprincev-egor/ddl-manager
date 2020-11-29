import { File } from "./File";

export class FilesState {
    readonly files: File[];
    
    constructor(files: File[] = []) {
        this.files = files;
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
}