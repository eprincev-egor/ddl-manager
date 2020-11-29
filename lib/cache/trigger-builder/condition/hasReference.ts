import { Expression, UnknownExpressionElement } from "../../../ast";
import { TableID } from "../../../database/schema/TableID";
import { CacheContext } from "../CacheContext";

export function hasReference(context: CacheContext) {
    return hasReferenceCondition(context, "is not null");
}

export function hasNoReference(context: CacheContext) {
    return hasReferenceCondition(context, "is null");
}

type CheckType = "is not null" | "is null";
function hasReferenceCondition(context: CacheContext, check: CheckType) {
    if ( !context.referenceMeta.expressions ) {
        return;
    }

    return buildReferenceExpression(
        context.referenceMeta.expressions,
        "and",
        context.triggerTable,
        check
    );
}

function buildReferenceExpression(
    expressions: Expression[],
    operator: "and" | "or",
    triggerTable: TableID,
    check: CheckType
): Expression {

    const referenceExpressions = expressions.map(expression => {

        const orConditions = expression.splitBy("or");
        if ( orConditions.length > 1 ) {
            return buildReferenceExpression(
                orConditions,
                "or",
                triggerTable,
                check
            );
        }

        return replaceSimpleExpressionToNotNulls(
            expression,
            triggerTable,
            check
        )
    });

    if ( operator === "and" ) {
        return Expression.and(referenceExpressions);
    }
    else {
        return Expression.or(referenceExpressions);
    }
}

function replaceSimpleExpressionToNotNulls(
    expression: Expression,
    triggerTable: TableID,
    check: CheckType
) {
    const notNullTriggerColumns = expression.getColumnReferences()
        .filter(columnRef =>
            columnRef.tableReference.table.equal(triggerTable)
        )
        .filter(columnRef =>
            columnRef.name !== "id"
        )
        .map(columnRef =>
            UnknownExpressionElement.fromSql(
                `${ columnRef } ${check}`,
                { [`${columnRef}`]: columnRef }
            )
        );
    
    return Expression.and(notNullTriggerColumns);
}