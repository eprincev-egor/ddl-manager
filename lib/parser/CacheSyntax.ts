import {
    AbstractScopeNode,
    Cursor,
    Name,
    TableReference,
    Select,
    TemplateElement, keyword, eol
} from "psql-lang";
import { CacheOption } from "./CacheOption";
import { CacheIndex } from "./CacheIndex";
import { CacheWithoutInsertOn } from "./CacheWithoutInsertOn";
import { CacheWithoutTriggersOn } from "./CacheWithoutTriggersOn";

export interface CacheRow {
    name: Name;
    for: TableReference;
    as?: Name;
    cache: Select;
    withoutTriggersOn?: TableReference[];
    withoutInsertOn?: TableReference[];
    indexes?: CacheIndex[];
}

export class CacheSyntax extends AbstractScopeNode<CacheRow> {

    static entry(cursor: Cursor): boolean {
        return cursor.beforeWord("cache");
    }

    static parse(cursor: Cursor): CacheRow {
        cursor.skipSpaces();
        cursor.readWord("cache");

        const name = cursor.parse(Name);
        cursor.skipSpaces();
        
        cursor.readWord("for");
        const destinationTable = cursor.parse(TableReference);
        cursor.skipSpaces();

        const alias = this.parseAlias(cursor);

        cursor.skipSpaces();
        cursor.readValue("(");
        cursor.skipSpaces();

        const cacheSelect = cursor.parse(Select);
        
        cursor.skipSpaces();
        cursor.readValue(")");
        cursor.skipSpaces();

        const options = this.parseOptions(cursor);

        return {
            name,
            for: destinationTable, as: alias,
            cache: cacheSelect,
            ...options
        };
    }

    private static parseAlias(cursor: Cursor) {
        if ( !cursor.beforeWord("as") ) {
            return;
        }

        cursor.readWord("as");
        return cursor.parse(Name);
    }

    private static parseOptions(cursor: Cursor) {
        if ( !cursor.before(CacheOption) ) {
            return {};
        }

        const indexes: CacheIndex[] = [];
        const withoutInsertOn: TableReference[] = [];
        const withoutTriggersOn: TableReference[] = [];

        const optionsNodes = cursor.parseChainOf(CacheOption);
        for (const optionNode of optionsNodes) {
            const option = optionNode.row.option;

            if ( option instanceof CacheIndex ) {
                indexes.push(option);
            }
            if ( option instanceof CacheWithoutInsertOn ) {
                withoutInsertOn.push(option.row.withoutInsertOn);
            }
            if ( option instanceof CacheWithoutTriggersOn ) {
                withoutTriggersOn.push(option.row.withoutTriggersOn);
            }
        }

        return {indexes, withoutInsertOn, withoutTriggersOn};
    }

    hasClojure() {
        return true;
    }

    template(): TemplateElement[] {
        const cache = this.row;
        const select = this.row.cache;

        const output: TemplateElement[] = [
            keyword("cache"), cache.name,
            keyword("for"), cache.for
        ];
        if ( cache.as ) {
            output.push(keyword("as"), cache.as);
        }

        output.push("(", eol);
        output.push(select, eol);
        output.push(")");

        for (const withoutInsertOn of cache.withoutInsertOn || []) {
            const node = new CacheWithoutInsertOn({row: {withoutInsertOn}})
            output.push(node, eol);
        }

        for (const withoutTriggersOn of cache.withoutTriggersOn || []) {
            const node = new CacheWithoutTriggersOn({row: {withoutTriggersOn}})
            output.push(node, eol);
        }

        for (const index of cache.indexes || []) {
            output.push(index, eol);
        }

        return output;
    }
}