import { ColumnReference } from "./ColumnReference";
import { FuncCall } from "./FuncCall";
import { IExpressionElement } from "./interface";
import { Table } from "../Table";
import { TableReference } from "../TableReference";
import { flatMap } from "../../utils";
import { AbstractAstElement } from "../AbstractAstElement";

export abstract class AbstractExpressionElement
extends AbstractAstElement
implements IExpressionElement {

    abstract clone(): IExpressionElement;
    // redefine me
    protected children(): IExpressionElement[] {
        return [];
    }

    equal(otherElem: IExpressionElement) {
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
        replaceTable: TableReference | Table,
        toTable: TableReference
    ) {
        return this.clone();
    }

    replaceColumn(replaceColumn: string, toSql: string) {
        return this.clone();
    }

    replaceFuncCall(replaceFunc: FuncCall, toSql: string) {
        return this.clone();
    }

}
