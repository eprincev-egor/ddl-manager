import { DatabaseFunction, DatabaseTrigger, Cache } from "../ast";

export interface IFileParams {
    name: string;
    folder: string;
    path: string;
    content: IFileContent;
}

export interface IFileContent {
    functions: DatabaseFunction[];
    triggers: DatabaseTrigger[];
    cache: Cache[];
}

export class File {
    readonly name!: string;
    readonly folder!: string;
    readonly path!: string;
    readonly content!: IFileContent;

    constructor(params: IFileParams) {
        Object.assign(this, params);
    }
}