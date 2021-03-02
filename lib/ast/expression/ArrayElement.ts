// cycle import
import { Expression } from "./Expression";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { ColumnReference } from "./ColumnReference";
import { IExpressionElement } from "./interface";
import { FuncCall } from "./FuncCall";
import { flatMap } from "lodash";

export class ArrayElement extends AbstractExpressionElement {

    readonly arrayContent: Expression[];
    constructor(arrayContent: Expression[]) {
        super();
        this.arrayContent = arrayContent;
    }

    protected children() {
        return flatMap(this.arrayContent, (expression) =>
            expression.children()
        );
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return this.clone(
            this.arrayContent.map(expression =>
                expression.replaceTable(replaceTable, toTable)
            )
        );
    }

    replaceColumn(replaceColumn: ColumnReference, toSql: IExpressionElement) {
        return this.clone(
            this.arrayContent.map(expression =>
                expression.replaceColumn(replaceColumn, toSql)
            )
        );
    }

    replaceFuncCall(replaceFunc: FuncCall, toSql: string) {
        return this.clone(
            this.arrayContent.map(expression =>
                expression.replaceFuncCall(replaceFunc, toSql)
            )
        );
    }

    clone(
        arrayContent = this.arrayContent.map(expression => 
            expression.clone()
        )
    ) {
        return new ArrayElement( arrayContent );
    }

    template() {
        return [`ARRAY[${ this.arrayContent.join(", ") }]`];
    }
}
