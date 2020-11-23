import {
    Table,
    Expression
} from "../../../ast";
import { Database as DatabaseStructure } from "../../schema/Database";

export function replaceArrayNotNullOn(
    sourceExpression: Expression | undefined,
    triggerTable: Table,
    databaseStructure: DatabaseStructure,
    funcName: string
): Expression | undefined {
    if ( !sourceExpression ) {
        return;
    }

    let outputExpression = sourceExpression;
    const tableStructure = databaseStructure.getTable(triggerTable);

    for (const columnRef of sourceExpression.getColumnReferences()) {
        if ( !columnRef.tableReference.table.equal(triggerTable) ) {
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
