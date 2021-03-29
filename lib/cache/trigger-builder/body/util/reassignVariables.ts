import { AbstractAstElement, AssignVariable, BlankLine, Expression, HardCode, If } from "../../../../ast";
import { IJoin } from "../../../processor/buildJoinVariables";
import { assignCombinedJoinVariables } from "./assignCombinedJoinVariables";
import { groupJoinsByTableAndFilter } from "./groupJoinsByTableAndFilter";
import { replaceTableToVariableOrRow } from "./replaceTableToVariableOrRow";

export function reassignVariables(newJoins: IJoin[], oldJoins: IJoin[]) {
    const lines: AbstractAstElement[] = [];

    const oldCombinedJoins = groupJoinsByTableAndFilter(oldJoins);
    const newCombinedJoins = groupJoinsByTableAndFilter(newJoins);

    for (let i = 0, n = newCombinedJoins.length; i < n; i++) {
        const newCombinedJoin = newCombinedJoins[i];
        const oldCombinedJoin = oldCombinedJoins[i];
        
        const newByColumn = replaceTableToVariableOrRow(
            newCombinedJoin.byColumn,
            newJoins,
            "new"
        );
        const oldByColumn = replaceTableToVariableOrRow(
            oldCombinedJoin.byColumn,
            oldJoins,
            "old"
        );

        lines.push(new If({
            if: Expression.and([
                newByColumn + " is not distinct from " + oldByColumn
            ]),
            then: newCombinedJoin.variables.map((newVarName, j) => 
                new AssignVariable({
                    variable: newVarName,
                    value: new HardCode({
                        sql: oldCombinedJoin.variables[j]
                    })
                })
            ),
            else: [
                new If({
                    if: Expression.and([
                        `${newByColumn} is not null`
                    ]),
                    then: assignCombinedJoinVariables(
                        newCombinedJoin,
                        newJoins,
                        "new"
                    )
                })
            ]
        }));
        
        lines.push( new BlankLine() );
    }
    
    return lines;
}
