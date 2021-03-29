import { AssignVariable, SimpleSelect } from "../../../../ast";
import { IJoin } from "../../../processor/buildJoinVariables";
import { ICombinedJoin } from "./groupJoinsByTableAndFilter";
import { replaceTableToVariableOrRow } from "./replaceTableToVariableOrRow";

export function assignCombinedJoinVariables(
    combinedJoin: ICombinedJoin,
    joins: IJoin[],
    row: "new" | "old"
) {
    if ( combinedJoin.variables.length === 1 ) {
        return [
            new AssignVariable({
                variable: combinedJoin.variables[0],
                value: new SimpleSelect({
                    columns: combinedJoin.joinedColumns,
                    from: combinedJoin.joinedTable.table,
                    where: replaceTableToVariableOrRow(
                        combinedJoin.byColumn,
                        joins,
                        row
                    )
                })
            })
        ]
    }
    
    return [
        new SimpleSelect({
            columns: combinedJoin.joinedColumns,
            into: combinedJoin.variables,
            from: combinedJoin.joinedTable.table,
            where: replaceTableToVariableOrRow(
                combinedJoin.byColumn,
                joins,
                row
            )
        })
    ];
}
