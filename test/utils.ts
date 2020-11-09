import {
    CacheFor,
    TableLink,
    GrapeQLCoach,
    FunctionBody
} from "grapeql-lang";
import { TableClause } from "../lib/TableClause";

export function formatTriggerBody(sql: string) {
    const breakAfter = [
        "then",
        "set",
        "or",
        "begin",
        "and",
        "else",
        "union"
    ];
    const breakBefore = [
        "where",
        "from",
        "or",
        "after",
        "else",
        "union"
    ];

    for (const keyword of breakAfter) {
        const regExp = new RegExp("\\s" + keyword + "\\s", "gi");
        sql = sql.replace(regExp, " " + keyword + "\n");
    }

    for (const keyword of breakBefore) {
        const regExp = new RegExp("\\s" + keyword + "\\s", "gi");
        sql = sql.replace(regExp, "\n" + keyword + " ");
    }

    sql = sql.replace(/;/g, ";\n");
    sql = sql.replace(/,/g, ",\n");

    return sql;
}

export function parseForTable(cache: CacheFor) {
    const forTable = cache.get("for") as TableLink;
    const alias = cache.get("as");

    const tableClause = new TableClause(
        forTable,
        alias
    );
    return tableClause;
}

export function parseCache(str: string) {
    const coach = new GrapeQLCoach(str)
    const cacheSyntax = coach.parse(CacheFor);
    return cacheSyntax;
}

export function parseFunctionBody(str: string) {
    const coach = new GrapeQLCoach(str)
    const bodySyntax = coach.parse(FunctionBody);
    return bodySyntax;
}
