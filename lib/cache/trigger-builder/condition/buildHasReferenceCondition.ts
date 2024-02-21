import { ColumnReference, Expression, IExpressionElement, UnknownExpressionElement } from "../../../ast";
import { CacheContext } from "../CacheContext";

export function buildHasReferenceCondition(context: CacheContext) {
    return buildCheckReferenceCondition(context, "is not null");
}

export function buildNoReferenceCondition(context: CacheContext) {
    return buildCheckReferenceCondition(context, "is null");
}

type CheckType = "is not null" | "is null";
function buildCheckReferenceCondition(context: CacheContext, check: CheckType) {
    if ( !context.referenceMeta.expressions ) {
        return;
    }

    return buildReferenceExpression(
        context.referenceMeta.expressions,
        "and",
        context,
        check
    );
}

function buildReferenceExpression(
    expressions: Expression[],
    operator: "and" | "or",
    context: CacheContext,
    check: CheckType
): Expression {

    const referenceExpressions = expressions.map(expression => {

        const orConditions = expression.splitBy("or");
        if ( orConditions.length > 1 ) {
            return buildReferenceExpression(
                orConditions,
                "or",
                context,
                check
            );
        }

        return replaceSimpleExpressionToNotNulls(
            expression,
            context,
            check
        )
    })
    .filter(expression =>
        !expression.isEmpty()
    );

    if ( operator === "and" ) {
        return Expression.and(referenceExpressions);
    }
    else {
        return Expression.or(referenceExpressions);
    }
}

function replaceSimpleExpressionToNotNulls(
    expression: Expression,
    context: CacheContext,
    check: CheckType
) {
    const triggerColumnsRefs = expression.getColumnReferences()
        .filter(columnRef =>
            context.isColumnRefToTriggerTable(columnRef)
        )
        .filter(columnRef =>
            columnRef.name !== "id"
        );
    
    const notNullTriggerColumns = triggerColumnsRefs.map(columnRef =>
        UnknownExpressionElement.fromSql(
            `${ columnRef } ${check}`,
            { [`${columnRef}`]: columnRef }
        )
    );
    
    // companies.id = any( orders.clients_ids || orders.partners_ids )
    // =>
    // new.clients_ids or new.partners_ids
    if ( expression.isEqualAny() ) {
        const [leftOperand, rightOperand] = expression.getOperands();
        const columnOperand = detectColumnOperand(leftOperand, rightOperand);
        const anyOperand = detectAnyOperand(leftOperand, rightOperand);

        if ( columnOperand && anyOperand ) {
            const arrOperand = executeAnyContent( anyOperand as UnknownExpressionElement );
            if ( /^\s*\w+\.\w+\s*\|\|\s*\w+\.\w+\s*$/.test(arrOperand.toString()) ) {
                return Expression.or(notNullTriggerColumns);
            }
        }
    }

    // companies.id in ( orders.id_client, orders.id_partner )
    // =>
    // new.id_client or new.id_partner
    if ( expression.isIn() ) {
        return Expression.or(notNullTriggerColumns);
    }

    return Expression.and(notNullTriggerColumns);
}



function detectColumnOperand(leftOperand: IExpressionElement, rightOperand: IExpressionElement) {
    if ( leftOperand instanceof ColumnReference ) {
        return leftOperand;
    }
    if ( rightOperand instanceof ColumnReference ) {
        return rightOperand;
    }
}

function detectAnyOperand(leftOperand: IExpressionElement, rightOperand: IExpressionElement) {
    if ( isAny(leftOperand) ) {
        return leftOperand;
    }
    if ( isAny(rightOperand) ) {
        return rightOperand;
    }
}

function isAny(operand: IExpressionElement) {
    return (
        /^any\s*\(.*\)$/.test( operand.toString().trim().toLowerCase() )
    );
}

function executeAnyContent(anyOperand: UnknownExpressionElement): IExpressionElement {
    const sql = anyOperand.toString()
        .trim()
        .replace(/^any\s*\(/, "")
        .replace(/\)$/, "");
    
    const arrOperand = UnknownExpressionElement.fromSql(
        sql,
        anyOperand.columnsMap
    );
    return arrOperand;
}