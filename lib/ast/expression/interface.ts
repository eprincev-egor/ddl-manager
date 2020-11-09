import { ColumnReference } from "./ColumnReference";
import { FuncCall } from "./FuncCall";
import { Table } from "../Table";
import { TableReference } from "../TableReference";
import { AbstractAstElement } from "../AbstractAstElement";

export interface IExpressionElement extends AbstractAstElement {
    clone(): IExpressionElement;
    getColumnReferences(): ColumnReference[];
    getFuncCalls(): FuncCall[];
    replaceColumn(replaceColumn: string, toSql: string): IExpressionElement;
    replaceTable(
        replaceTable: TableReference | Table,
        toTable: TableReference
    ): IExpressionElement;
    replaceFuncCall(replaceFunc: FuncCall, toSql: string): IExpressionElement;
    toString(): string;
}