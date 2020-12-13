import { IJoinMeta } from "./findJoinsMeta";
import { IJoin } from "../trigger-builder/body/buildCommutativeBodyWithJoins";
import { Database } from "../../database/schema/Database";
import { TableID } from "../../database/schema/TableID";

export function buildJoins(
    database: Database,
    joins: IJoinMeta[],
    row: "new" | "old"
) {
    return joins.map(meta => 
        buildJoin(database, meta, row)
    );
}

function buildJoin(database: Database, meta: IJoinMeta, row: "new" | "old"): IJoin {
    const byColumn = meta.joinByColumn.split(".")[1];

    const tableId = TableID.fromString(meta.joinedTable);
    const table = database.getTable( tableId );
    const column = table && table.getColumn(meta.joinedColumn);
    

    const join: IJoin = {
        variable: {
            name: [
                row,
                byColumn.replace("id_", ""),
                meta.joinedColumn
            ].join("_"),

            type: column && column.type.toString() || "text"
        },
        table: {
            alias: meta.joinAlias,
            name: meta.joinedTable,
            column: meta.joinedColumn
        },
        on: {
            column: byColumn
        }
    };
    return join;
}