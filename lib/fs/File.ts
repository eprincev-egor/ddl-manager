import { Cache } from "../ast";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { CacheIndex } from "../ast/CacheIndex";

export interface IFileParams {
    name: string;
    folder: string;
    path: string;
    content: Partial<IFileContent>;
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
        if ( !this.content.functions ) {
            this.content.functions = [];
        }
        if ( !this.content.triggers ) {
            this.content.triggers = [];
        }
        if ( !this.content.cache ) {
            this.content.cache = [];
        }
    }
}