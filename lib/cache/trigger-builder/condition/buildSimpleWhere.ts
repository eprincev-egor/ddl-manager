import {
    Expression,
    TableReference
} from "../../../ast";
import { replaceOperatorAnyToIndexedOperator } from "./replaceOperatorAnyToIndexedOperator";
import { replaceAmpArrayToAny } from "./replaceAmpArrayToAny";
import { CacheContext } from "../CacheContext";

export function buildSimpleWhere(
    context: CacheContext,
    row: "new" | "old"
) {
    const linksToTriggerTable = context.cache.select.findTableReferences(context.triggerTable);

    const conditions = context.referenceMeta.expressions.map(expression => {

        linksToTriggerTable.forEach((linkToTriggerTable) => {

            expression = expression.replaceTable(
                linkToTriggerTable,
                new TableReference(
                    context.triggerTable,
                    row
                )
            );
        });

        expression = replaceOperatorAnyToIndexedOperator(
            context.cache,
            expression
        );
        expression = replaceAmpArrayToAny(
            context.cache,
            expression
        );

        return expression;
    });

    const where = Expression.and(conditions);
    if ( !where.isEmpty() ) {
        return where;
    }
}
