import { TableID } from "./TableID";
import { Comment } from "./Comment";

interface IndexParams {
    name: string;
    table: TableID;
    index: string;
    columns: string[];
    comment?: Comment;
}

export class Index {
    readonly name: string;
    readonly table: TableID;
    readonly index: string;
    readonly columns: readonly string[];
    readonly comment: Comment;

    constructor(params: IndexParams) {
        this.name = params.name;
        this.table = params.table;
        this.index = params.index;
        this.columns = params.columns;
        this.comment = params.comment || Comment.frozen("index");
    }

    equal(otherIndex: Index) {
        return (
            this.name === otherIndex.name &&
            this.index === otherIndex.index &&
            this.table.equal(otherIndex.table) &&
            this.comment.equal(otherIndex.comment) &&
            this.columns.toString() == otherIndex.columns.toString()
        );
    }

    getSignature() {
        return this.table.schema + "." + this.name;
    }

    toSQL() {
        return `
            create index ${ this.name }
            on ${ this.table }
            using ${ this.index }
            ( ${ this.columns.join(", ") } )
        `;
    }
}