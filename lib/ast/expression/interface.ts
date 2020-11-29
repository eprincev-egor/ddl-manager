import { ColumnReference } from "./ColumnReference";
import { FuncCall } from "./FuncCall";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { AbstractAstElement } from "../AbstractAstElement";

export interface IExpressionElement extends AbstractAstElement {
    clone(): IExpressionElement;
    getColumnReferences(): ColumnReference[];
    getFuncCalls(): FuncCall[];
    replaceColumn(replaceColumn: string, toSql: string): IExpressionElement;
    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ): IExpressionElement;
    replaceFuncCall(replaceFunc: FuncCall, toSql: string): IExpressionElement;
    toString(): string;
}