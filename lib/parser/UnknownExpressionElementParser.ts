import { AbstractNode, ColumnReference as ColumnLink } from "psql-lang";
import { ColumnReferenceParser } from "./ColumnReferenceParser";
import {
    UnknownExpressionElement,
    IColumnsMap,
    Select
} from "../ast";
import { TableReference } from "../database/schema/TableReference"
import { flatMap } from "lodash";

export class UnknownExpressionElementParser {

    private columnReferenceParser = new ColumnReferenceParser();

    parse(
        select: Select,
        additionalTableReferences: TableReference[],
        syntax: AbstractNode<any>
    ) {
        const columnsMap = this.buildColumnsMap(select, additionalTableReferences, syntax);
        return new UnknownExpressionElement(syntax, columnsMap);
    }

    buildColumnsMap(
        select: Select,
        additionalTableReferences: TableReference[],
        syntax: AbstractNode<any> | AbstractNode<any>[]
    ) {
        const syntaxes = (
            Array.isArray(syntax) ? 
                syntax : [syntax]
        );
        const columnsMap: IColumnsMap = {};

        const columnLinks = flatMap(syntaxes, scanColumnLinks);

        columnLinks.forEach(columnLink => {
            const columnReference = this.columnReferenceParser.parse(
                select, 
                additionalTableReferences, 
                columnLink
            );

            columnsMap[ columnLink.toString() ] = columnReference;
        });

        return columnsMap
    }
}

function scanColumnLinks(syntax: AbstractNode<any>) {
    const columnLinks = syntax.filterChildrenByInstance(ColumnLink);

    if ( syntax instanceof ColumnLink ) {
        columnLinks.push(syntax);
    }

    return columnLinks;
}