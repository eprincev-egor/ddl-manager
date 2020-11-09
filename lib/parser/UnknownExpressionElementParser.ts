import { ColumnLink } from "grapeql-lang";
import { ColumnReferenceParser } from "./ColumnReferenceParser";
import {
    UnknownExpressionElement,
    IUnknownSyntax,
    IColumnsMap,
    Select,
    TableReference 
} from "../ast";

export class UnknownExpressionElementParser {

    private columnReferenceParser = new ColumnReferenceParser();

    parse(
        select: Select,
        additionalTableReferences: TableReference[],
        syntax: IUnknownSyntax
    ) {
        const columnsMap: IColumnsMap = {};
        
        const columnLinks = (syntax as any).filterChildrenByInstance(ColumnLink) as ColumnLink[];
        columnLinks.forEach(columnLink => {
            const columnReference = this.columnReferenceParser.parse(
                select, 
                additionalTableReferences, 
                columnLink
            );

            columnsMap[ columnLink.toString() ] = columnReference;
        });

        return new UnknownExpressionElement(syntax, columnsMap);
    }
}