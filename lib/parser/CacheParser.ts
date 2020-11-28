import { 
    GrapeQLCoach, 
    CacheFor, 
    ObjectName, 
    TableLink, 
    Select as SelectSyntax
} from "grapeql-lang";
import { TableReferenceParser } from "./TableReferenceParser";
import { SelectParser } from "./SelectParser";
import { Cache, TableReference } from "../ast";

export class CacheParser {

    static parse(cacheSQL: string | GrapeQLCoach) {
        const parser = new CacheParser(cacheSQL);
        return parser.parse();
    }

    private syntax: CacheFor;
    private selectParser: SelectParser;
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
    }

    parse() {
        const forTable = this.tableParser.parse(
            this.syntax.get("for") as TableLink,
            this.syntax.get("as")
        );
        const cache = new Cache(
            this.parseCacheName(),
            forTable,
            this.parseSelect(forTable),
            this.parseWithoutTriggers()
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
}
