import {
    AbstractNode, Cursor, TemplateElement,
    keyword, TableReference
} from "psql-lang";

export interface CacheWithoutTriggersOnRow {
    withoutTriggersOn: TableReference;
}

export class CacheWithoutTriggersOn extends AbstractNode<CacheWithoutTriggersOnRow> {

    static entry(cursor: Cursor): boolean {
        return (
            cursor.beforePhrase("without", "triggers") ||
            cursor.beforePhrase("without", "trigger")
        );
    }

    static parse(cursor: Cursor): CacheWithoutTriggersOnRow {
        this.parseEntry(cursor);

        const withoutTriggersOn = cursor.parse(TableReference);
        return {withoutTriggersOn};
    }

    private static parseEntry(cursor: Cursor) {
        cursor.readWord("without");

        if ( cursor.beforeWord("trigger") ) {
            cursor.readWord("trigger");
        }
        else {
            cursor.readWord("triggers");
        }

        cursor.readWord("on");
    }

    template(): TemplateElement[] {
        return [
            ...["without", "triggers", "on"].map(keyword),
            this.row.withoutTriggersOn
        ];
    }
}