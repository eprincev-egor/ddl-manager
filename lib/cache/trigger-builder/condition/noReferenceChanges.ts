import {
    Table
} from "../../../ast";
import { IReferenceMeta } from "./buildReferenceMeta";
import { Database as DatabaseStructure } from "../../schema/Database";
import { noChanges } from "./noChanges";

export function noReferenceChanges(
    referenceMeta: IReferenceMeta,
    triggerTable: Table,
    databaseStructure: DatabaseStructure
) {
    const importantColumns = referenceMeta.columns.slice();

    for (const filter of referenceMeta.filters) {
        const filterColumns = filter.getColumnReferences().map(columnRef =>
            columnRef.name
        );
        importantColumns.push( ...filterColumns );
    }

    return noChanges(
        importantColumns,
        triggerTable,
        databaseStructure
    );
}
