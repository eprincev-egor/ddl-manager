
import {
    AbstractNode, Cursor, TemplateElement,
    keyword, TableReference
} from "psql-lang";

export interface CacheWithoutInsertOnRow {
    withoutInsertOn: TableReference;
}

export class CacheWithoutInsertOn extends AbstractNode<CacheWithoutInsertOnRow> {

    static entry(cursor: Cursor): boolean {
        return cursor.beforePhrase("without", "insert");
    }

    static parse(cursor: Cursor): CacheWithoutInsertOnRow {
        cursor.readPhrase("without", "insert", "case", "on");

        const withoutInsertOn = cursor.parse(TableReference);
        return {withoutInsertOn};
    }

    template(): TemplateElement[] {
        return [
            ...["without", "insert", "case", "on"].map(keyword),
            this.row.withoutInsertOn
        ];
    }
}