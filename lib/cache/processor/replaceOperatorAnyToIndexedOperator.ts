import {
    Cache,
    Expression,
    Operator,
    ColumnReference,
    IExpressionElement,
    UnknownExpressionElement
} from "../../ast";

// try using gin-index scan
// input (cannot use gin-index):
//     orders.id = any (companies.order_ids)
// output (can use gin-index):
//     array[ orders.id_client ] && companies.order_ids
export function replaceOperatorAnyToIndexedOperator(
    cache: Cache,
    input: Expression
): Expression {
    if ( !input.isBinary("=") ) {
        return input;
    }

    const [leftOperand, rightOperand] = input.getOperands();

    const columnOperand = detectColumnOperand(leftOperand, rightOperand);
    const anyOperand = detectAnyOperand(leftOperand, rightOperand);
    if ( !columnOperand || !anyOperand ) {
        return input;
    }

    if ( columnOperand.tableReference.equal(cache.for) ) {
        return input;
    }

    const arrOperand = executeAnyContent( anyOperand as UnknownExpressionElement );

    const output = new Expression([
        arrOperand,
        new Operator("&&"),
        // TODO: cast bigint to same type with array
        // array[]::bigint[] && some_bigint_ids
        UnknownExpressionElement.fromSql(
            `ARRAY[ ${ columnOperand } ]`,
            {[columnOperand.toString()]: columnOperand}
        )
    ]);
    return output;
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