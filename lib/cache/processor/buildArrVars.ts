import { IArrVar } from "../trigger-builder/body/buildCommutativeBody";
import { CacheContext } from "../trigger-builder/CacheContext";

export function buildArrVars(context: CacheContext, row: string): IArrVar[] {
    const dbTable = context.database.getTable(context.triggerTable);

    const arrVars: IArrVar[] = [];
    const prefix = row === "new" ? "inserted" : "deleted";

    context.referenceMeta.columns.forEach(columnName => {
        const dbColumn = dbTable && dbTable.getColumn(columnName);

        if ( dbColumn && dbColumn.type.isArray() ) {
            arrVars.push({
                name: prefix + "_" + columnName,
                type: dbColumn.type.toString(),
                triggerColumn: columnName
            });
        }
    });

    return arrVars;
}