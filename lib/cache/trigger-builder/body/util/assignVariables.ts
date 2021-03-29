import { AbstractAstElement, BlankLine, Expression, If } from "../../../../ast";
import { IJoin } from "../../../processor/buildJoinVariables";
import { assignCombinedJoinVariables } from "./assignCombinedJoinVariables";
import { groupJoinsByTableAndFilter } from "./groupJoinsByTableAndFilter";
import { replaceTableToVariableOrRow } from "./replaceTableToVariableOrRow";

export function assignVariables(joins: IJoin[], row: "new" | "old") {
    const lines: AbstractAstElement[] = [];

    const combinedJoins = groupJoinsByTableAndFilter(joins);

    for (const combinedJoin of combinedJoins) {
        lines.push(
            new If({
                if: Expression.and([
                    replaceTableToVariableOrRow(
                        combinedJoin.byColumn,
                        joins,
                        row
                    ) + " is not null"
                ]),
                then: assignCombinedJoinVariables(
                    combinedJoin,
                    joins,
                    row
                )
            })
        );

        lines.push( new BlankLine() );
    }

    return lines;
}
