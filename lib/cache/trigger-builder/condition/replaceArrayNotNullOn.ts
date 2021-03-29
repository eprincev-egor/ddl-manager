import {
    Expression, UnknownExpressionElement
} from "../../../ast";
import { IArrVar } from "../../processor/buildArrVars";
import { CacheContext } from "../CacheContext";

export function replaceArrayNotNullOn(
    context: CacheContext,
    sourceExpression: Expression | undefined,
    arrVars: IArrVar[]
): Expression | undefined {
    if ( !sourceExpression ) {
        return;
    }
    if ( !arrVars.length ) {
        return sourceExpression;
    }

    let outputExpression = sourceExpression;
    const tableStructure = context.database.getTable(context.triggerTable);

    for (const columnRef of sourceExpression.getColumnReferences()) {
        if ( !columnRef.tableReference.table.equal(context.triggerTable) ) {
            continue;
        }

        const column = tableStructure && tableStructure.getColumn(columnRef.name);
        if ( column && column.type.isArray() ) {
            const arrVar = arrVars.find(someVar =>
                someVar.triggerColumn === columnRef.name
            );

            if ( arrVar ) {
                outputExpression = outputExpression.replaceColumn(
                    columnRef,
                    UnknownExpressionElement.fromSql(
                        `${arrVar.name}`
                    )
                );
            }
        }
    }

    return outputExpression;
}
