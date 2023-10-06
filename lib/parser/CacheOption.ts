import { AbstractNode, Cursor, TemplateElement } from "psql-lang";
import { CacheIndex } from "./CacheIndex";
import { CacheWithoutInsertOn } from "./CacheWithoutInsertOn";
import { CacheWithoutTriggersOn } from "./CacheWithoutTriggersOn";

export type CacheOptionRow = {
    option: CacheIndex | CacheWithoutInsertOn | CacheWithoutTriggersOn
};

export class CacheOption extends AbstractNode<CacheOptionRow> {

    static entry(cursor: Cursor): boolean {
        return (
            cursor.before(CacheIndex) ||
            cursor.before(CacheWithoutInsertOn) ||
            cursor.before(CacheWithoutTriggersOn)
        );
    }

    static parse(cursor: Cursor): CacheOptionRow {
        const option = this.parseOption(cursor);
        return {option};
    }

    private static parseOption(cursor: Cursor) {
        if ( cursor.before(CacheIndex) ) {
            return cursor.parse(CacheIndex);
        }

        if ( cursor.before(CacheWithoutInsertOn) ) {
            return cursor.parse(CacheWithoutInsertOn);
        }

        return cursor.parse(CacheWithoutTriggersOn);
    }

    template(): TemplateElement[] {
        return [this.row.option];
    }
}