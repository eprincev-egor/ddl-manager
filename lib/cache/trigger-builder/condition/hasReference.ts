import { Expression, Table, UnknownExpressionElement } from "../../../ast";
import { CacheContext } from "../CacheContext";

export function hasReference(context: CacheContext) {
    if ( !context.referenceMeta.expressions ) {
        return;
    }

    return buildReferenceExpression(
        context.referenceMeta.expressions,
        "and",
        context.triggerTable
    );
}

function buildReferenceExpression(
    expressions: Expression[],
    operator: "and" | "or",
    triggerTable: Table
): Expression {

    const referenceExpressions = expressions.map(expression => {

        const orConditions = expression.splitBy("or");
        if ( orConditions.length > 1 ) {
            return buildReferenceExpression(
                orConditions,
                "or",
                triggerTable
            );
        }

        return replaceSimpleExpressionToNotNulls(
            expression,
            triggerTable
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
    triggerTable: Table
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
                `${ columnRef } is not null`,
                { [`${columnRef}`]: columnRef }
            )
        );
    
    return Expression.and(notNullTriggerColumns);
}