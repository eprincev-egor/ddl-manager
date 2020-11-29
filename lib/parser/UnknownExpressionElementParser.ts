import { ColumnLink } from "grapeql-lang";
import { ColumnReferenceParser } from "./ColumnReferenceParser";
import {
    UnknownExpressionElement,
    IUnknownSyntax,
    IColumnsMap,
    Select
} from "../ast";
import { TableReference } from "../database/schema/TableReference"

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