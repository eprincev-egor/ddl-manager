import {
    Cache,
    Expression,
    FuncCall
} from "../../../ast";
import { createAggValue } from "../../processor/createAggValue";
import { IJoinMeta } from "../../processor/findJoinsMeta";
import { CacheContext } from "../CacheContext";

type RowType = "new" | "old";

export function hasEffect(
    context: CacheContext,
    row: RowType,
    joinsMeta: IJoinMeta[]
) {
    const aggCalls = findAggCalls(context.cache);

    const hasCountAgg = aggCalls.some(aggCall => aggCall.name === "count");
    const hasArrayAgg = aggCalls.some(aggCall => aggCall.name === "array_agg");
    if ( hasArrayAgg || hasCountAgg ) {
        return;
    }

    const aggCallsForMutableColumns = aggCalls.filter(aggCall => {
        const isIdColumn = aggCall.args[0]
            .getColumnReferences()
            .some(col => col.name === "id");
        
        return !isIdColumn;
    });
    
    const effects: string[] = [];
    for (const aggCall of aggCallsForMutableColumns) {

        const aggValue = createAggValue(
            context.triggerTable,
            joinsMeta,
            aggCall.args,
            row
        );

        let effect = `${aggValue} is not null`;
        if ( aggCall.name === "sum" ) {
            effect = `coalesce(${aggValue}, 0) != 0`;
        }

        if ( !effects.includes(effect) ) {
            effects.push(effect);
        }
    }

    if ( !effects.length ) {
        return;
    }

    return Expression.or(effects);
}

function findAggCalls(cache: Cache) {
    const aggCalls: FuncCall[] = [];

    for (const column of cache.select.columns) {
        for (const aggCall of column.getAggregations()) {
            aggCalls.push(aggCall);
        }
    }

    return aggCalls;
}