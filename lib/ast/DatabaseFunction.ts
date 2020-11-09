import { DatabaseFunctionType } from "../interface";

export class DatabaseFunction  {
    schema!: string;
    name!: string;
    returns!: {
        type?: string;
        setof?: boolean;
        table?: any[];
    };
    frozen?: boolean;
    comment?: string;

    constructor(json: DatabaseFunctionType) {
        Object.assign(this, json);
    }
}