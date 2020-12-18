import assert from "assert";
import {
    Cache,
    Expression,
    Operator,
    ColumnReference,
    IExpressionElement,
    UnknownExpressionElement,
    IColumnsMap
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

    const columnRefs = arrOperand.getColumnReferences();
    const isCacheId = (
        columnRefs.length === 1 &&
        columnRefs[0].name === "id" &&
        columnRefs[0].tableReference.equal(cache.for)
    )
    if ( !isCacheId ) {
        return input;
    }

    const arrContent = executeArrayContent( arrOperand );
    const castingSQL = executeArrayCasting( arrOperand );

    const output = new Expression([
        arrContent,
        new Operator("="),
        // TODO: cast bigint to same type with array
        UnknownExpressionElement.fromSql(
            `any( ${ columnOperand }${ castingSQL } )`,
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
    const operandSQL = operand.toString().trim().toLowerCase();
    return (
        /^array\s*\[.*\](\s*::\s*(\w+)\[\])?$/.test( operandSQL )
    );
}

function executeArrayContent(anyOperand: IExpressionElement): IExpressionElement {
    const matchResult = anyOperand.toString().match(/\[([^\]]+)\]/) as string[];
    assert.ok(matchResult && matchResult[1]);

    const columnsMap: IColumnsMap = {};
    if ( anyOperand instanceof UnknownExpressionElement ) {
        Object.assign(columnsMap, anyOperand.columnsMap);
    }
    else if ( anyOperand instanceof Expression ) {
        for (const subElem of anyOperand.elements) {
            const subMap = (subElem as any).columnsMap || {};
            Object.assign(columnsMap, subMap);
        }
    }

    const arrContentSQL = matchResult[1].trim();
    const arrContentElem = UnknownExpressionElement.fromSql(
        arrContentSQL,
        columnsMap
    );
    return arrContentElem;
}

function executeArrayCasting(anyOperand: IExpressionElement): string {
    if ( !(anyOperand instanceof Expression) ) {
        return "";
    }

    if ( anyOperand.isBinary("::") ) {
        const castType = anyOperand.elements[2] as any;
        return `::${ castType }`;
    }

    return "";
}