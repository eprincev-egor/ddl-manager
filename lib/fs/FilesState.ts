import { File } from "./File";

export class FilesState {
    files: File[];
    
    constructor(files: File[] = []) {
        this.files = files;
    }
}