import { ColumnReference } from "./ColumnReference";
import { FuncCall } from "./FuncCall";
import { IExpressionElement } from "./interface";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { flatMap } from "lodash";
import { AbstractAstElement } from "../AbstractAstElement";

export abstract class AbstractExpressionElement
extends AbstractAstElement
implements IExpressionElement {

    abstract clone(): IExpressionElement;
    // redefine me
    protected children(): IExpressionElement[] {
        return [];
    }

    equal(otherElem: this) {
        return this.toString() === otherElem.toString();
    }

    getColumnReferences(): ColumnReference[] {
        return flatMap(this.children(), (elem: IExpressionElement) =>
            elem.getColumnReferences()
        );
    }

    getFuncCalls(): FuncCall[] {
        return flatMap(this.children(), (elem: IExpressionElement) =>
            elem.getFuncCalls()
        );
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return this.clone();
    }

    replaceColumn(replaceColumn: ColumnReference, toSql: IExpressionElement) {
        return this.clone();
    }

    replaceFuncCall(replaceFunc: FuncCall, toSql: string) {
        return this.clone();
    }

}
