import { Expression } from "../../../ast";
import { IJoinMeta } from "../../processor/findJoinsMeta";
import { hasEffect } from "./hasEffect";
import { hasReference } from "./hasReference";
import { CacheContext } from "../CacheContext";
import { flatMap } from "lodash";

export function buildNeedUpdateCondition(
    context: CacheContext,
    row: "new" | "old",
    joinsMeta: IJoinMeta[] = []
) {
    const conditions = [
        hasReference(context),
        hasEffect(context, row, joinsMeta),
        Expression.and(context.referenceMeta.filters),
        matchedAllAggFilters(context)
    ].filter(condition => 
        condition != null &&
        !condition.isEmpty()
    ) as Expression[];

    const needUpdate = Expression.and(conditions);
    if ( !needUpdate.isEmpty() ) {
        return needUpdate;
    }
}

function matchedAllAggFilters(context: CacheContext) {

    const allAggCalls = flatMap(
        context.cache.select.columns, 
        column => column.getAggregations()
    );
    const everyAggCallHaveFilter = allAggCalls.every(aggCall => aggCall.where != null);
    if ( !everyAggCallHaveFilter ) {
        return;
    }

    const filterConditions = allAggCalls.map(aggCall => {
        const expression = aggCall.where as Expression;
        return expression;
    });

    return Expression.or(filterConditions);
}
