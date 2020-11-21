import {
    Cache,
    Table,
    TableReference,
    Expression
} from "../../../ast";
import { flatMap } from "lodash";

export function matchedAllAggFilters(
    cache: Cache,
    triggerTable: Table,
    row: "new" | "old"
) {

    const allAggCalls = flatMap(cache.select.columns, column => column.getAggregations());
    const everyAggCallHaveFilter = allAggCalls.every(aggCall => aggCall.where != null);
    if ( !everyAggCallHaveFilter ) {
        return;
    }

    const filterConditions = allAggCalls.map(aggCall => {
        let expression = aggCall.where as Expression;
        
        expression = expression.replaceTable(
            triggerTable,
            new TableReference(
                triggerTable,
                row
            )
        );

        return expression.toString();
    });

    return Expression.or(filterConditions);
}
