import {
    TableReference,
    Expression
} from "../../../ast";
import { IJoinMeta } from "../../processor/findJoinsMeta";
import { hasEffect } from "./hasEffect";
import { hasReference } from "./hasReference";
import { matchedAllAggFilters } from "./matchedAllAggFilters";
import { CacheContext } from "../CacheContext";

export function buildNeedUpdateCondition(
    context: CacheContext,
    row: "new" | "old",
    joinsMeta: IJoinMeta[] = []
) {
    const conditions = [
        hasReference(context, row),
        hasEffect(context, row, joinsMeta),
        matchedFilter(context, row),
        matchedAllAggFilters(context, row)
    ].filter(condition => 
        condition != null &&
        !condition.isEmpty()
    ) as Expression[];

    const needUpdate = Expression.and(conditions);
    if ( !needUpdate.isEmpty() ) {
        return needUpdate;
    }
}

function matchedFilter(
    context: CacheContext,
    row: "new" | "old"
) {
    if ( !context.referenceMeta.filters.length ) {
        return;
    }

    const filterConditions = context.referenceMeta.filters.map(filter =>
        filter.replaceTable(
            context.triggerTable,
            new TableReference(
                context.triggerTable,
                row
            )
        ).toString()
    );

    return Expression.and(filterConditions);
}
