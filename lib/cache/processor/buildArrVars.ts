import { CacheContext } from "../trigger-builder/CacheContext";

export interface IArrVar {
    name: string;
    type: string;
    triggerColumn: string;
}

export function buildArrVars(context: CacheContext, prefix: string): IArrVar[] {
    const dbTable = context.database.getTable(context.triggerTable);

    const arrVars: IArrVar[] = [];

    context.referenceMeta.columns.forEach(columnName => {
        const dbColumn = dbTable && dbTable.getColumn(columnName);

        if ( dbColumn && dbColumn.type.isArray() ) {
            arrVars.push({
                name: prefix + columnName,
                type: dbColumn.type.toString(),
                triggerColumn: columnName
            });
        }
    });

    return arrVars;
}