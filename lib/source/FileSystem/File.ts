import { TDBO } from "../../common";

export interface IFileParams {
    name: string;
    path: string;
    sql: string;
    objects: TDBO[];
}

export class File {
    name: string;
    path: string;
    sql: string;
    objects: TDBO[];

    constructor(params: IFileParams) {
        this.name = params.name;
        this.path = params.path;
        this.sql = params.sql;
        this.objects = params.objects;
    }
}