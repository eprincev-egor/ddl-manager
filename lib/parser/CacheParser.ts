import {
    Sql,
    ColumnReference,
    TableReference as TableLink
} from "psql-lang";
import { SelectParser } from "./SelectParser";
import { Cache, Select } from "../ast";
import { CacheIndex, IndexTarget } from "../ast/CacheIndex";
import { ExpressionParser } from "./ExpressionParser";
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
    private expressionParser: ExpressionParser;
    private constructor(input: string | Sql | CacheSyntax) {
        this.selectParser = new SelectParser();
        this.expressionParser = new ExpressionParser();

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
            this.syntax.row.as?.toString()
        );
        const select = this.parseSelect(forTable);

        const cache = new Cache(
            this.syntax.row.name.toString(),
            forTable,
            select,
            this.parseTables(this.syntax.row.withoutTriggersOn),
            this.parseTables(this.syntax.row.withoutInsertOn),
            this.parseIndexes(select, forTable)
        );
        return cache;
    }

    private parseSelect(cacheFor: TableReference) {
        const selectSyntax = this.syntax.row.cache;
        return this.selectParser.parse(cacheFor, selectSyntax);
    }

    private parseTables(tables: TableLink[] = []) {
        return tables.map(table =>
            parseFromTable(table).table.toString()
        );
    }

    private parseIndexes(select: Select, cacheFor: TableReference) {
        const indexesSyntaxes = this.syntax.row.indexes || [];
        const indexes = indexesSyntaxes.map(cacheIndexSyntax => {
            const index = cacheIndexSyntax.row.index;
            const onSyntaxes = cacheIndexSyntax.row.columns || [];

            const on: IndexTarget[] = onSyntaxes.map(onSyntax => {
                if ( onSyntax.row.expression instanceof ColumnReference ) {
                    return onSyntax.row.expression.toString();
                }

                const expression = this.expressionParser.parse(
                    select,
                    [cacheFor],
                    onSyntax.row.expression.toString()
                );
                return expression;
            });

            const cacheIndex = new CacheIndex(
                index,
                on
            );
            return cacheIndex;
        });

        return indexes;
    }
}
