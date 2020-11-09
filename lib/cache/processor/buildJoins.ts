import { IJoinMeta } from "./findJoinsMeta";
import { IJoin } from "./buildCommutativeBodyWithJoins";

export function buildJoins(
    joins: IJoinMeta[],
    row: "new" | "old"
) {
    return joins.map(meta => 
        buildJoin(meta, row)
    );
}

function buildJoin(meta: IJoinMeta, row: "new" | "old"): IJoin {
    const byColumn = meta.joinByColumn.split(".")[1];

    const join: IJoin = {
        variable: {
            name: [
                row,
                byColumn.replace("id_", ""),
                meta.joinedColumn
            ].join("_"),
            
            // TODO: load types
            type: "text"
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