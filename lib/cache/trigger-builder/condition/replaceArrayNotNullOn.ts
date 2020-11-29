import {
    Expression
} from "../../../ast";
import { CacheContext } from "../CacheContext";

export function replaceArrayNotNullOn(
    context: CacheContext,
    sourceExpression: Expression | undefined,
    funcName: string
): Expression | undefined {
    if ( !sourceExpression ) {
        return;
    }

    let outputExpression = sourceExpression;
    const tableStructure = context.database.getTable(context.triggerTable);

    for (const columnRef of sourceExpression.getColumnReferences()) {
        if ( !columnRef.tableReference.table.equal(context.triggerTable) ) {
            continue;
        }

        const column = tableStructure && tableStructure.getColumn(columnRef.name);
        if ( column && column.type.isArray() ) {
            outputExpression = outputExpression.replaceColumn(
                columnRef.toString(),
                `${funcName}(old.${column.name}, new.${column.name})`
            );
        }
    }

    return outputExpression;
}
