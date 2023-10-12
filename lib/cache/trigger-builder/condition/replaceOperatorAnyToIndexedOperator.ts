import {
    Expression,
    Operator,
    ColumnReference,
    IExpressionElement,
    UnknownExpressionElement
} from "../../../ast";
import { TableReference } from "../../../database/schema/TableReference";

// try using gin-index scan
// input (cannot use gin-index):
//     from companies where orders.id = any(companies.order_ids)
// output (can use gin-index):
//     from companies where array[ orders.id_client ] && companies.order_ids
export function replaceOperatorAnyToIndexedOperator(
    cacheFor: TableReference,
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

    if ( columnOperand.tableReference.equal(cacheFor) ) {
        return input;
    }

    const arrOperand = executeAnyContent( anyOperand as UnknownExpressionElement );

    let arrayLiteral = `ARRAY[${columnOperand}]`;

    const arrColumns = arrOperand.getColumnReferences();
    if ( arrColumns.length === 1 ) {
        const table = arrColumns[0].tableReference.table;
        const columnName = arrColumns[0].name;

        arrayLiteral = `cm_build_array_for((
            select ${columnName}
            from ${table.schema}.${table.name}
            where false
        ), ${columnOperand})`;
    }

    const output = new Expression([
        arrOperand,
        new Operator("&&"),
        // TODO: cast bigint to same type with array
        // array[]::bigint[] && some_bigint_ids
        UnknownExpressionElement.fromSql(
            arrayLiteral,
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