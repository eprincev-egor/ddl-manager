import { TableID } from "./TableID";
import { Type } from "./Type";
import { Comment } from "./Comment";
import { MAX_NAME_LENGTH } from "../postgres/constants";

export class Column {
    readonly table: TableID;
    readonly name: string;
    readonly type: Type;
    readonly default: string | null;
    readonly cacheSignature?: string;
    readonly frozen?: boolean;
    comment: Comment;

    constructor(
        table: TableID,
        name: string,
        type: string,
        defaultValue?: string | null,
        comment?: Comment
    ) {
        this.table = table;

        // if ( name.length > MAX_NAME_LENGTH ) {
        //     // tslint:disable-next-line: no-console
        //     console.error(`name "${name}" too long (> 64 symbols)`);
        // }
        this.name = name.slice(0, MAX_NAME_LENGTH);
        this.name = name;

        this.type = new Type(type);
        this.default = defaultValue || null;
        this.comment = comment || Comment.frozen("column");
        this.cacheSignature = this.comment.cacheSignature;
        this.frozen = this.comment.frozen;
    }

    getSignature() {
        return this.table.toString() + "." + this.name;
    }

    toJSON() {
        return {
            table: {
                schema: this.table.schema,
                name: this.table.name
            },
            name: this.name,
            type: this.type.value,
            "default": this.default,
            cacheSignature: this.cacheSignature,
            comment: this.comment.toString()
        };
    }

    equalName(column: {name: string} | string) {
        const otherName = typeof column === "string" ? 
            column : column.name;

        return equalColumnName(this.name, otherName);
    }

    same(newColumn: Column) {
        return (
            this.type.suit(newColumn.type) &&
            formatDefault({
                type: this.type.value,
                default: this.default
            }) === formatDefault({
                type: newColumn.type.value,
                default: newColumn.default
            }) &&
            this.comment.equal( newColumn.comment )
        );
    }

    clone() {
        return new Column(
            this.table,
            this.name,
            this.type.toString(),
            this.default || undefined,
            this.comment
        );
    }

    isFrozen() {
        return this.comment.frozen;
    }

    markAsFrozen() {
        this.comment = this.comment.markAsFrozen();
    }
}

export function formatDefault(arg: {type?: string, default?: string | null}) {
    let someDefault = ("" + arg.default).trim().toLowerCase();

    someDefault = someDefault.replace(/\s*::\s*([\w\s]+|numeric\([\d\s,]+\))(\[])?$/, "");
    someDefault = someDefault.trim();

    if ( someDefault === "{}" ) {
        someDefault = "'{}'";
    }

    return someDefault;
}

export function equalColumnName(nameA: string, nameB: string) {
    return (
        nameA.slice(0, MAX_NAME_LENGTH) ==
        nameB.slice(0, MAX_NAME_LENGTH)
    )
}