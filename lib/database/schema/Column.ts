import { Type } from "./Type";

export class Column {
    readonly name: string;
    readonly type: Type;

    constructor(name: string, type: string) {
        this.name = name;
        this.type = new Type(type);
    }
}