import {
    Expression,
    Table
} from "../../../ast";
import { Database as DatabaseStructure } from "../../schema/Database";
import { Table as TableStructure } from "../../schema/Table";
import { Column } from "../../schema/Column";
import assert from "assert";

export function noChanges(
    columns: string[],
    triggerTable: Table,
    databaseStructure: DatabaseStructure
) {
    const mutableColumns = columns.filter(column =>
        column !== "id"
    );

    const tableStructure = databaseStructure.getTable(triggerTable) as TableStructure;
    // assert.ok(tableStructure, `table ${ triggerTable.toString() } does not exists`);

    const conditions: string[] = [];
    for (const columnName of mutableColumns) {
        const column = tableStructure && tableStructure.getColumn(columnName) as Column;
        // assert.ok(column, `column ${ triggerTable.toString() }.${ columnName } does not exists`);

        if ( column && column.type.isArray() ) {
            conditions.push(`not cm_is_distinct_arrays(new.${columnName}, old.${columnName})`);
        }
        else {
            conditions.push(`new.${ columnName } is not distinct from old.${ columnName }`);
        }
    }
    
    const noChangesCondition = Expression.and(conditions);
    return noChangesCondition;
}