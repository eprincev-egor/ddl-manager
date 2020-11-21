import {
    Cache,
    Table,
    TableReference,
    Expression
} from "../../../ast";
import { IReferenceMeta } from "./buildReferenceMeta";
import { IJoinMeta } from "../findJoinsMeta";
import { hasEffect } from "./hasEffect";
import { hasReference } from "./hasReference";
import { matchedAllAggFilters } from "./matchedAllAggFilters";

export function buildNeedUpdateCondition(
    cache: Cache,
    triggerTable: Table,
    referenceMeta: IReferenceMeta,
    row: "new" | "old",
    joinsMeta: IJoinMeta[] = []
) {
    const conditions = [
        hasReference(triggerTable, referenceMeta, row),
        hasEffect(cache, triggerTable, row, joinsMeta),
        matchedFilter(triggerTable, referenceMeta.filters, row),
        matchedAllAggFilters(cache, triggerTable, row)
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
    triggerTable: Table,
    filters: Expression[],
    row: "new" | "old"
) {
    if ( !filters.length ) {
        return;
    }

    const filterConditions = filters.map(filter =>
        filter.replaceTable(
            triggerTable,
            new TableReference(
                triggerTable,
                row
            )
        ).toString()
    );

    return Expression.and(filterConditions);
}
