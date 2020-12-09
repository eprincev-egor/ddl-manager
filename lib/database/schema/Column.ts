import { TableID } from "./TableID";
import { Type } from "./Type";
import { Comment } from "./Comment";

export class Column {
    readonly table: TableID;
    readonly name: string;
    readonly type: Type;
    readonly default: string | null;
    readonly comment: Comment;
    readonly cacheSignature?: string;

    constructor(
        table: TableID,
        name: string,
        type: string,
        defaultValue?: string,
        comment?: Comment
    ) {
        this.table = table;
        this.name = name;
        this.type = new Type(type);
        this.default = defaultValue || null;
        this.comment = comment || Comment.frozen("column");
        this.cacheSignature = this.comment.cacheSignature;
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

    equal(otherColumn: Column) {
        return (
            this.name === otherColumn.name &&
            this.table.equal( otherColumn.table ) &&
            this.type.toString() === otherColumn.type.toString() &&
            this.default === otherColumn.default &&
            this.comment.equal( otherColumn.comment )
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
}