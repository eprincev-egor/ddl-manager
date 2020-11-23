import {
    TableReference,
    Expression
} from "../../../ast";
import { flatMap } from "lodash";
import { CacheContext } from "../CacheContext";

export function matchedAllAggFilters(
    context: CacheContext,
    row: "new" | "old"
) {

    const allAggCalls = flatMap(
        context.cache.select.columns, 
        column => column.getAggregations()
    );
    const everyAggCallHaveFilter = allAggCalls.every(aggCall => aggCall.where != null);
    if ( !everyAggCallHaveFilter ) {
        return;
    }

    const filterConditions = allAggCalls.map(aggCall => {
        let expression = aggCall.where as Expression;
        
        expression = expression.replaceTable(
            context.triggerTable,
            new TableReference(
                context.triggerTable,
                row
            )
        );

        return expression.toString();
    });

    return Expression.or(filterConditions);
}
