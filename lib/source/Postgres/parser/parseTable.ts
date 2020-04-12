import {
    GrapeQLCoach,
    CreateTable
} from "grapeql-lang";
import { TableDBO } from "../objects/TableDBO";
import { parseDeprecatedColumns } from "./table/parseDeprecatedColumns";
import { parseColumns } from "./table/parseColumns";
import { parseConstraints } from "./table/parseConstraints";
import { parseValues } from "./table/parseValues";

export function parseTable(coach: GrapeQLCoach) {
    const gqlTable = coach.parse(CreateTable);
    const tableIdentify = parseTableIdentify(gqlTable);

    const tableDBO = new TableDBO({
        deprecated: gqlTable.get("deprecated"),
        deprecatedColumns: parseDeprecatedColumns( gqlTable.get("deprecatedColumns") ),

        schema: gqlTable.get("schema").toString(),
        name: gqlTable.get("name").toString(),
        columns: parseColumns(tableIdentify, gqlTable.row.columns),
        
        constraints: parseConstraints(tableIdentify, gqlTable.get("constraints")),
        values: parseValues(gqlTable.get("valuesRows")),
        inherits: parseInherits(gqlTable)
    });

    return tableDBO;
}

function parseTableIdentify(gqlTable: CreateTable) {
    const schema = gqlTable.get("schema");
    const tableName = gqlTable.get("name");
    const tableIdentify = (schema || "public").toString() + "." + tableName.toString();

    return tableIdentify;
}

function parseInherits(gqlTable: CreateTable) {
    return gqlTable.get("inherits")
        .map((tableLink) =>
            tableLink.toString()
        );
}