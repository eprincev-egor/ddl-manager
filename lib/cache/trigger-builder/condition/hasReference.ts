import { Expression, Table, TableReference, UnknownExpressionElement } from "../../../ast";
import { CacheContext } from "../CacheContext";

type RowType = "new" | "old";

export function hasReference(
    context: CacheContext,
    row: RowType
) {
    if ( !context.referenceMeta.expressions ) {
        return;
    }

    return buildReferenceExpression(
        context.referenceMeta.expressions,
        "and",
        context.triggerTable,
        row
    );
}

function buildReferenceExpression(
    expressions: Expression[],
    operator: "and" | "or",
    triggerTable: Table,
    row: RowType
): Expression {

    const referenceExpressions = expressions.map(expression => {

        const orConditions = expression.splitBy("or");
        if ( orConditions.length > 1 ) {
            return buildReferenceExpression(
                orConditions,
                "or",
                triggerTable,
                row
            );
        }

        return replaceSimpleExpressionToNotNulls(
            expression,
            triggerTable,
            row
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
    triggerTable: Table,
    row: RowType
) {
    const notNullTriggerColumns = expression.getColumnReferences()
        .filter(columnRef =>
            columnRef.tableReference.table.equal(triggerTable)
        )
        .filter(columnRef =>
            columnRef.name !== "id"
        )
        // TODO: format it
        .map(columnRef =>
            UnknownExpressionElement.fromSql(
                `${row}.${ columnRef.name } is not null`,
                {
                    [`${row}.${columnRef.name}`]: columnRef.replaceTable(
                        columnRef.tableReference,
                        new TableReference(
                            columnRef.tableReference.table,
                            row
                        )
                    )
                }
            )
        );
    
    return Expression.and(notNullTriggerColumns);
}