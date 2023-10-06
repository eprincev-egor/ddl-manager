import {
    AbstractNode, Cursor, TemplateElement,
    OrderByItem,
    keyword, _, printChain
} from "psql-lang";

export interface CacheIndexRow {
    index: "btree" | "gin";
    columns: OrderByItem[];
}

export class CacheIndex extends AbstractNode<CacheIndexRow> {

    static entry(cursor: Cursor): boolean {
        return cursor.beforeWord("index");
    }

    static parse(cursor: Cursor): CacheIndexRow {
        cursor.readWord("index");

        const index = this.parseIndexType(cursor);

        cursor.readWord("on");

        cursor.readValue("(");
        cursor.skipSpaces();

        const columns = cursor.parseChainOf(OrderByItem, ",");

        cursor.skipSpaces();
        cursor.readValue(")");
        cursor.skipSpaces();

        return {index, columns};
    }

    private static parseIndexType(cursor: Cursor): CacheIndexRow["index"] {
        if ( cursor.beforeWord("btree") ) {
            cursor.readWord("btree");
            return "btree";
        }

        if ( cursor.beforeWord("gin") ) {
            cursor.readWord("gin");
            return "gin";
        }

        cursor.throwError("expected index type: btree or gin");
    }

    template(): TemplateElement[] {
        return [
            keyword("index"), keyword(this.row.index),
            keyword("on"), _, "(", ...printChain(this.row.columns, ",", _), ")"
        ];
    }
}