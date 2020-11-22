import {
    Cache,
    Expression,
    Operator,
    ColumnReference,
    IExpressionElement,
    UnknownExpressionElement
} from "../../../ast";

// try using btree-index scan
// input (cannot use btree-index):
//     new.companies_ids && array[ companies.id ]
// output (can use btree-index):
//     companies.id = any( new.companies_ids )
export function replaceAmpArrayToAny(
    cache: Cache,
    input: Expression
): Expression {
    if ( !input.isBinary("&&") ) {
        return input;
    }

    const [leftOperand, rightOperand] = input.getOperands();

    const columnOperand = detectColumnOperand(leftOperand, rightOperand);
    const arrOperand = detectArrOperand(leftOperand, rightOperand);
    if ( !columnOperand || !arrOperand ) {
        return input;
    }
    const arrContent = executeArrayContent( arrOperand as UnknownExpressionElement );

    const columnRefs = arrContent.getColumnReferences();
    const isCacheId = (
        columnRefs.length === 1 &&
        columnRefs[0].name === "id" &&
        columnRefs[0].tableReference.equal(cache.for)
    )
    if ( !isCacheId ) {
        return input;
    }

    const output = new Expression([
        arrContent,
        new Operator("="),
        // TODO: cast bigint to same type with array
        UnknownExpressionElement.fromSql(
            `any( ${ columnOperand } )`,
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

function detectArrOperand(leftOperand: IExpressionElement, rightOperand: IExpressionElement) {
    if ( isArrayExpression(leftOperand) ) {
        return leftOperand;
    }
    if ( isArrayExpression(rightOperand) ) {
        return rightOperand;
    }
}

function isArrayExpression(operand: IExpressionElement) {
    return (
        /^array\s*\[.*\]$/.test( operand.toString().trim().toLowerCase() )
    );
}

function executeArrayContent(anyOperand: UnknownExpressionElement): IExpressionElement {
    const sql = anyOperand.toString()
        .trim()
        .replace(/^array\s*\[/, "")
        .replace(/\]$/, "");
    
    const arrOperand = UnknownExpressionElement.fromSql(
        sql,
        anyOperand.columnsMap
    );
    return arrOperand;
}