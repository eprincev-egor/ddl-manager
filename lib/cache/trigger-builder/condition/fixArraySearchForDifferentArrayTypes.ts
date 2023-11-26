import {
    Expression,
    Operator,
    ColumnReference,
    IExpressionElement,
    UnknownExpressionElement,
    IColumnsMap
} from "../../../ast";
import { TableReference } from "../../../database/schema/TableReference";

// form_table.x && cache_row.y
// =>
// form_table.x && cast(cache_row.y as ...)
export function fixArraySearchForDifferentArrayTypes(
    fromTable: TableReference,
    where: Expression
): Expression | undefined {
    const conditions = where.splitBy("and").map(condition => 
        fixArraySearchForDifferentArrayTypesInCondition(fromTable, condition)
    );
    
    return Expression.and(conditions);
}

export function fixArraySearchForDifferentArrayTypesInCondition(
    fromTable: TableReference | undefined,
    condition: Expression
) {
    if ( !fromTable || !condition.isBinary("&&") ) {
        return condition;
    }

    const [leftOperand, rightOperand] = condition.getOperands();
    const fromTableColumn = detectFromTableColumn(fromTable, leftOperand, rightOperand);
    const otherOperand = detectOther(fromTableColumn, leftOperand, rightOperand);

    if ( !fromTableColumn || !otherOperand ) {
        return condition;
    }

    const table = fromTableColumn.tableReference.table;
    const columnName = fromTableColumn.name;

    return new Expression([
        fromTableColumn,
        new Operator("&&"),
        // array[]::bigint[] && some_bigint_ids
        UnknownExpressionElement.fromSql(
            `cm_build_array_for((null::${table.schema}.${table.name}).${columnName}, ${otherOperand})`,
            getColumnReferencesMap(otherOperand)
        )
    ]);
}

function detectFromTableColumn(
    fromTable: TableReference,
    left: IExpressionElement,
    right: IExpressionElement
): ColumnReference | undefined {
    if ( left instanceof ColumnReference && left.isFrom(fromTable) ) {
        return left;
    }

    if ( right instanceof ColumnReference && right.isFrom(fromTable) ) {
        return right;
    }
}

function detectOther(
    other: IExpressionElement | undefined,
    left: IExpressionElement,
    right: IExpressionElement
) {
    if ( left === other ) {
        return right;
    }
    if ( right === other ) {
        return left;
    }
}

function getColumnReferencesMap(operand: IExpressionElement) {
    const map: IColumnsMap = {};
    for (const columnRef of operand.getColumnReferences()) {
        map[ columnRef.toString() ] = columnRef;
    }
    return map;
}