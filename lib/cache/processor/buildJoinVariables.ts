import { IJoinMeta } from "./findJoinsMeta";
import { IJoin } from "../trigger-builder/body/buildCommutativeBody";
import { Database } from "../../database/schema/Database";

export function buildJoinVariables(
    database: Database,
    joins: IJoinMeta[],
    row: string
) {
    const variables: IJoin[] = [];

    for (const meta of joins) {
        const byColumn = meta.joinByColumn;

        const tableRef = meta.joinedTable;
        const table = database.getTable( tableRef.table );

        for (const columnRef of meta.joinedColumns) {
            const column = table && table.getColumn(columnRef.name);

            const joinVariable: IJoin = {
                variable: {
                    name: [
                        row,
                        byColumn.name.replace("id_", ""),
                        columnRef.name
                    ].join("_"),
        
                    type: column && column.type.toString() || "text"
                },
                table: {
                    ref: meta.joinedTable,
                    column: columnRef
                },
                on: {
                    column: byColumn
                }
            };
    
            variables.push(joinVariable);
        }
    }

    return variables;
}
