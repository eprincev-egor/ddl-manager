import { TableID } from "./TableID";
import { Type } from "./Type";
import { Comment } from "./Comment";
import { MAX_NAME_LENGTH } from "../postgres/constants";

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
        defaultValue?: string | null,
        comment?: Comment
    ) {
        this.table = table;

        if ( name.length > MAX_NAME_LENGTH ) {
            // tslint:disable-next-line: no-console
            console.error(`name "${name}" too long (> 64 symbols)`);
        }
        this.name = name.slice(0, MAX_NAME_LENGTH);
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

    suit(newColumn: Column) {
        return (
            this.type.suit(newColumn.type) &&
            equalDefaultValues(this.default, newColumn.default) &&
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
}

function equalDefaultValues(default1: string | null, default2: string | null) {
    if ( default1 == null ) {
        default1 = "null";
    }
    if ( default2 == null ) {
        default2 = "null";
    }
    return default1 === default2;
}