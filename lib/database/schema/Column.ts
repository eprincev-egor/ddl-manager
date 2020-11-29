import { Type } from "./Type";

export class Column {
    readonly name: string;
    readonly type: Type;
    readonly default: string | null;
    readonly comment?: string;

    constructor(
        name: string,
        type: string,
        defaultValue?: string,
        comment?: string
    ) {
        this.name = name;
        this.type = new Type(type);
        this.default = defaultValue || null;
        this.comment = comment;
    }
}