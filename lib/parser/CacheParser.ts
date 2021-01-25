import { 
    GrapeQLCoach, 
    CacheFor, 
    ObjectName, 
    TableLink, 
    Select as SelectSyntax
} from "grapeql-lang";
import { TableReferenceParser } from "./TableReferenceParser";
import { SelectParser } from "./SelectParser";
import { Cache, Select } from "../ast";
import { TableReference } from "../database/schema/TableReference";
import { Expression as ExpressionSyntax } from "grapeql-lang";
import { CacheIndex } from "../ast/CacheIndex";
import { ExpressionParser } from "./ExpressionParser";

export class CacheParser {

    static parse(cacheSQL: string | GrapeQLCoach) {
        const parser = new CacheParser(cacheSQL);
        return parser.parse();
    }

    private syntax: CacheFor;
    private selectParser: SelectParser;
    private expressionParser: ExpressionParser;
    private tableParser: TableReferenceParser;
    private constructor(cacheSQLOrCoach: string | GrapeQLCoach) {

        let coach: GrapeQLCoach = cacheSQLOrCoach as GrapeQLCoach;
        if ( typeof cacheSQLOrCoach === "string" ) {
            const cacheSQL = cacheSQLOrCoach;
            coach = new GrapeQLCoach(cacheSQL);
        }

        this.syntax = coach.parse(CacheFor);
        this.selectParser = new SelectParser();
        this.tableParser = new TableReferenceParser();
        this.expressionParser = new ExpressionParser();
    }

    parse() {
        const forTable = this.tableParser.parse(
            this.syntax.get("for") as TableLink,
            this.syntax.get("as")
        );
        const select = this.parseSelect(forTable);

        const cache = new Cache(
            this.parseCacheName(),
            forTable,
            select,
            this.parseWithoutTriggers(),
            this.parseIndexes(select, forTable)
        );
        return cache;
    }

    private parseCacheName() {
        const nameSyntax = this.syntax.get("name") as ObjectName;
        const name = nameSyntax.toLowerCase() as string;
        return name;
    }

    private parseSelect(cacheFor: TableReference) {
        const selectSyntax = this.syntax.get("cache") as SelectSyntax;
        const select = this.selectParser.parse(cacheFor, selectSyntax);
        return select;
    }

    private parseWithoutTriggers() {
        const withoutTriggersSyntaxes = this.syntax.get("withoutTriggers") || [];
        const withoutTriggers = withoutTriggersSyntaxes.map(onTableSyntax =>
            this.tableParser.parse(onTableSyntax).table.toString()
        );
        return withoutTriggers;
    }

    private parseIndexes(select: Select, cacheFor: TableReference) {
        const indexesSyntaxes = this.syntax.get("indexes") || [];
        const indexes = indexesSyntaxes.map(cacheIndexSyntax => {
            const index = cacheIndexSyntax.get("index") as string;
            const onSyntaxes = cacheIndexSyntax.get("on") || [];

            const on = onSyntaxes.map(onSyntax => {
                if ( onSyntax instanceof ObjectName ) {
                    return onSyntax.toString();
                }

                const expressionSyntax = onSyntax as ExpressionSyntax;
                const expression = this.expressionParser.parse(
                    select,
                    [cacheFor],
                    expressionSyntax
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
