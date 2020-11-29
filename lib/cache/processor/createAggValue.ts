import { Expression } from "../../ast";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { buildJoins } from "./buildJoins";
import { IJoinMeta } from "./findJoinsMeta";

export function createAggValue(
    triggerTable: TableID,
    joinsMeta: IJoinMeta[],
    aggArgs: Expression[],
    row: "new" | "old"
): Expression {
    let valueExpression = aggArgs[0];

    if ( joinsMeta.length ) {
        const joins = buildJoins(joinsMeta, row);
        
        joins.forEach((join) => {
            valueExpression = valueExpression.replaceColumn(
                (join.table.alias || join.table.name) + "." + join.table.column,
                join.variable.name
            );
        });
    }

    valueExpression = valueExpression.replaceTable(
        triggerTable,
        new TableReference(
            triggerTable,
            row
        )
    );
    return valueExpression;
}
