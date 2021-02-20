import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { ColumnReference } from "./ColumnReference";
import { Expression } from "./Expression";
import { IExpressionElement } from "./interface";

export class CoalesceFalseExpression extends AbstractExpressionElement {
    readonly condition: Expression;

    constructor(condition: Expression) {
        super();
        this.condition = condition;
    }

    template() {
        return [`coalesce(${ this.condition }, false)`];
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return new CoalesceFalseExpression(
            this.condition.replaceTable(
                replaceTable,
                toTable
            )
        );
    }

    replaceColumn(replaceColumn: ColumnReference, toSql: IExpressionElement) {
        return new CoalesceFalseExpression(
            this.condition.replaceColumn(
                replaceColumn,
                toSql
            )
        );
    }

    clone() {
        return new CoalesceFalseExpression(this.condition.clone());
    }

    protected children() {
        return this.condition.children();
    }
}