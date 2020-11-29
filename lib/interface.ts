import {
    Cache,
    DatabaseTrigger,
    DatabaseFunction 
} from "./ast";

export interface IFileContent {
    functions: DatabaseFunction[];
    triggers: DatabaseTrigger[];
    cache: Cache[];
}

export interface IFile {
    name: string;
    folder: string;
    path: string;
    content: IFileContent;
}
