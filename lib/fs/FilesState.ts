import { File, IFileParams } from "./File";

export class FilesState {
    readonly files: File[];
    
    constructor(files: File[] = []) {
        this.files = files;
    }

    addFile(fileOrParams: File | IFileParams) {
        let file!: File;
        if ( fileOrParams instanceof File ) {
            file = fileOrParams;
        }
        else {
            file = new File(fileOrParams);
        }

        this.files.push(file);
    }

    removeFile(file: File) {
        const fileIndex = this.files.indexOf(file);
        if ( fileIndex !== -1 ) {
            this.files.splice(fileIndex, 1);
        }
    }
}