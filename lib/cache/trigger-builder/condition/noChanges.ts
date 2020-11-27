import {
    Expression
} from "../../../ast";
import { Table as TableStructure } from "../../../database/schema/Table";
import { Column } from "../../../database/schema/Column";
import { CacheContext } from "../CacheContext";

export function noChanges(
    context: CacheContext,
    columns?: string[]
) {
    const mutableColumns = (columns || context.triggerTableColumns).filter(column =>
        column !== "id"
    );

    const tableStructure = context.databaseStructure.getTable(
        context.triggerTable
    ) as TableStructure;
    // assert.ok(tableStructure, `table ${ triggerTable.toString() } does not exists`);

    const conditions: string[] = [];
    for (const columnName of mutableColumns) {
        const column = tableStructure && tableStructure.getColumn(columnName) as Column;
        // assert.ok(column, `column ${ triggerTable.toString() }.${ columnName } does not exists`);

        if ( column && column.type.isArray() ) {
            conditions.push(`cm_equal_arrays(new.${columnName}, old.${columnName})`);
        }
        else {
            conditions.push(`new.${ columnName } is not distinct from old.${ columnName }`);
        }
    }
    
    const noChangesCondition = Expression.and(conditions);
    return noChangesCondition;
}