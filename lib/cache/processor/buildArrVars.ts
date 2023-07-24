import { uniq } from "lodash";
import { CacheContext } from "../trigger-builder/CacheContext";
import { findTriggerTableArrayColumns } from "./findTriggerTableArrayColumns";

export interface IArrVar {
    name: string;
    type: string;
    triggerColumn: string;
}

export function buildArrVars(
    context: CacheContext,
    prefix: string = "__"
): IArrVar[] {

    const arrayColumns = findTriggerTableArrayColumns(
        context.cache,
        context.triggerTable,
        context.referenceMeta.expressions
    );

    const dbTable = context.database.getTable(context.triggerTable);
    context.referenceMeta.columns.forEach(columnName => {
        const dbColumn = dbTable && dbTable.getColumn(columnName);

        if ( dbColumn && dbColumn.type.isArray() ) {
            arrayColumns.push(columnName);
        }
    });

    const arrVars: IArrVar[] = [];
    uniq(arrayColumns).forEach(columnName => {
        const dbColumn = dbTable && dbTable.getColumn(columnName);

        arrVars.push({
            name: prefix + columnName,
            type: dbColumn ? dbColumn.type.toString() : "bigint[]",
            triggerColumn: columnName
        });
    });

    return arrVars;
}
