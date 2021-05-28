// cycle import
import { Expression } from "./Expression";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { ColumnReference } from "./ColumnReference";
import { IExpressionElement } from "./interface";
import { FuncCall } from "./FuncCall";

export class Extract extends AbstractExpressionElement {

    readonly extract: string;
    readonly from: Expression;
    constructor(
        extract: string,
        from: Expression
    ) {
        super();
        this.extract = extract;
        this.from = from;
    }

    protected children() {
        return this.from.children();
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return this.clone(
            this.from.replaceTable(replaceTable, toTable)
        );
    }

    replaceColumn(replaceColumn: ColumnReference, toSql: IExpressionElement) {
        return this.clone(
            this.from.replaceColumn(replaceColumn, toSql)
        );
    }

    replaceFuncCall(replaceFunc: FuncCall, toSql: string) {
        return this.clone(
            this.from.replaceFuncCall(replaceFunc, toSql)
        );
    }

    clone(from = this.from.clone()) {
        return new Extract( this.extract, from );
    }

    template() {
        return [`extract( ${ this.extract } from ${ this.from })`];
    }
}
