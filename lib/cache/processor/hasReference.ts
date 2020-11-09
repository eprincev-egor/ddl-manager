import { Expression, Table } from "../../ast";
import { IReferenceMeta } from "./buildReferenceMeta";

type RowType = "new" | "old";

export function hasReference(
    triggerTable: Table,
    referenceMeta: IReferenceMeta,
    row: RowType
) {
    if ( !referenceMeta.expressions ) {
        return;
    }

    return buildReferenceExpression(
        referenceMeta.expressions,
        "and",
        triggerTable,
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
        .map(columnRef =>
            `${row}.${ columnRef.name } is not null`
        );
    
    return Expression.and(notNullTriggerColumns);
}