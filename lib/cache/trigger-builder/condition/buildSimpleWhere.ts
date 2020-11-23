import { Expression } from "../../../ast";
import { replaceOperatorAnyToIndexedOperator } from "./replaceOperatorAnyToIndexedOperator";
import { replaceAmpArrayToAny } from "./replaceAmpArrayToAny";
import { CacheContext } from "../CacheContext";

export function buildSimpleWhere(
    context: CacheContext
) {

    const conditions = context.referenceMeta.expressions.map(expression => {

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
