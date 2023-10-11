import {
    Sql,
    TableReference as TableLink
} from "psql-lang";
import { SelectParser } from "./SelectParser";
import { Cache } from "../ast";
import { CacheIndex } from "../ast/CacheIndex";
import { CacheSyntax } from "./CacheSyntax";
import { TableID } from "../database/schema/TableID";
import { TableReference } from "../database/schema/TableReference";
import { DEFAULT_SCHEMA } from "./defaults";
import { parseFromTable } from "./utils";

export class CacheParser {

    static parse(cacheSQL: string | Sql | CacheSyntax) {
        const parser = new CacheParser(cacheSQL);
        return parser.parse();
    }

    private syntax: CacheSyntax;
    private selectParser: SelectParser;
    private constructor(input: string | Sql | CacheSyntax) {
        this.selectParser = new SelectParser();

        if ( input instanceof CacheSyntax ) {
            this.syntax = input;
        }
        else if ( input instanceof Sql ) {
            this.syntax = input.parse(CacheSyntax);
        }
        else {
            this.syntax = Sql.code(input).parse(CacheSyntax);
        }
    }

    parse() {
        const forTable = new TableReference(
            new TableID(
                this.syntax.row.for.row.schema?.toValue() || DEFAULT_SCHEMA,
                this.syntax.row.for.row.name.toValue()
            ),
            this.syntax.row.as?.toValue()
        );
        const select = this.parseSelect();

        const cache = new Cache(
            this.syntax.row.name.toValue(),
            forTable,
            select,
            this.parseTables(this.syntax.row.withoutTriggersOn),
            this.parseTables(this.syntax.row.withoutInsertOn),
            this.parseIndexes()
        );
        return cache;
    }

    private parseSelect() {
        const selectSyntax = this.syntax.row.cache;
        return this.selectParser.parse(selectSyntax);
    }

    private parseTables(tables: TableLink[] = []) {
        return tables.map(table =>
            parseFromTable(table).table.toString()
        );
    }

    private parseIndexes() {
        return this.syntax.row.indexes?.map(cacheIndexSyntax => {
            const index = cacheIndexSyntax.row.index;
            const onSyntaxes = cacheIndexSyntax.row.columns || [];

            const on = onSyntaxes.map(onSyntax => 
                onSyntax.row.expression.toString()
            );

            return new CacheIndex(
                index,
                on
            );
        });
    }
}
