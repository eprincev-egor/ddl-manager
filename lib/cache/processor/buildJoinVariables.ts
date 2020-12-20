import { IJoinMeta } from "./findJoinsMeta";
import { IJoin } from "../trigger-builder/body/buildCommutativeBody";
import { Database } from "../../database/schema/Database";
import { TableID } from "../../database/schema/TableID";

export function buildJoinVariables(
    database: Database,
    joins: IJoinMeta[],
    row: "new" | "old"
) {
    const variables: IJoin[] = [];

    for (const meta of joins) {
        const byColumn = meta.joinByColumn.split(".")[1];

        const tableId = TableID.fromString(meta.joinedTable);
        const table = database.getTable( tableId );

        for (const columnName of meta.joinedColumns) {
            const column = table && table.getColumn(columnName);

            const joinVariable: IJoin = {
                variable: {
                    name: [
                        row,
                        byColumn.replace("id_", ""),
                        columnName
                    ].join("_"),
        
                    type: column && column.type.toString() || "text"
                },
                table: {
                    alias: meta.joinAlias,
                    name: meta.joinedTable,
                    column: columnName
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
