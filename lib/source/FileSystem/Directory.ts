import { File } from "./File";

export class Directory {
    name: string;
    files: File[];
    directories: Directory[];
}