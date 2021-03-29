import { ColumnReference } from "../../../../ast";
import { IJoin } from "../../../processor/buildJoinVariables";

export function replaceTableToVariableOrRow(
    columnRef: ColumnReference,
    joins: IJoin[],
    row: "new" | "old"
) {
    const sourceJoin = joins.find(join =>
        join.table.ref.equal(columnRef.tableReference) &&
        join.table.column.name === columnRef.name
    );
    if ( sourceJoin ) {
        return sourceJoin.variable.name;
    }

    return `${row}.${columnRef.name}`
}
