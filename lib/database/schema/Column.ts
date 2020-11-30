import { TableID } from "./TableID";
import { Type } from "./Type";

export class Column {
    readonly table: TableID;
    readonly name: string;
    readonly type: Type;
    readonly default: string | null;
    readonly comment?: string;
    readonly cacheSignature?: string;

    constructor(
        table: TableID,
        name: string,
        type: string,
        defaultValue?: string,
        comment?: string,
        cacheSignature?: string
    ) {
        this.table = table;
        this.name = name;
        this.type = new Type(type);
        this.default = defaultValue || null;
        this.comment = comment;
        this.cacheSignature = cacheSignature;
    }

    getSignature() {
        return this.table.toString() + "." + this.name;
    }
}