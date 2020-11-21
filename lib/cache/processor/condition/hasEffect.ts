import {
    Cache,
    Table,
    Expression,
    FuncCall
} from "../../../ast";
import { createAggValue } from "../createAggValue";
import { IJoinMeta } from "../findJoinsMeta";

type RowType = "new" | "old";

export function hasEffect(
    cache: Cache,
    triggerTable: Table,
    row: RowType,
    joinsMeta: IJoinMeta[]
) {
    const aggCalls = findAggCalls(cache);

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
            triggerTable,
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