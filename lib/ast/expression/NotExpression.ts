import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { Expression } from "./Expression";

export class NotExpression extends AbstractExpressionElement {
    readonly not: Expression;

    constructor(not: Expression) {
        super();
        this.not = not;
    }

    template() {
        return [`not coalesce(${ this.not }, false)`];
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return new NotExpression(
            this.not.replaceTable(replaceTable, toTable)
        );
    }

    clone() {
        return new NotExpression(this.not.clone());
    }
}