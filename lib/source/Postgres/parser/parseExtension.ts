import {
    GrapeQLCoach,
    Extension as GQLExtension
} from "grapeql-lang";
import { Extension as PGExtension } from "../objects/Extension";
import { parseDeprecatedColumns } from "./table/parseDeprecatedColumns";
import { parseColumns } from "./table/parseColumns";
import { parseConstraints } from "./table/parseConstraints";
import { parseValues } from "./table/parseValues";

export function parseExtension(coach: GrapeQLCoach): PGExtension {
    const gqlExtension = coach.parse(GQLExtension);
    const forTableIdentify = gqlExtension.get("forTable").toString();

    const extension = new PGExtension({
        deprecated: gqlExtension.get("deprecated"),
        deprecatedColumns: parseDeprecatedColumns( gqlExtension.get("deprecatedColumns") ),

        columns: parseColumns(forTableIdentify, gqlExtension.row.columns),
        
        constraints: parseConstraints(forTableIdentify, gqlExtension.get("constraints")),
        values: parseValues(gqlExtension.get("valuesRows"))
    });

    return extension;
}
