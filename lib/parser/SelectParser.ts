import {
    Select as SelectSyntax,
    FromTable,
    TableReference as TableLink,
    Join as JoinSyntax
} from "psql-lang";
import { ExpressionParser } from "./ExpressionParser";
import {
    From, Join, Select,
    SelectColumn,
    OrderBy, OrderByItem
} from "../ast";
import { parseFromTable } from "./utils";

export class SelectParser {

    private expressionParser = new ExpressionParser();
    
    parse(selectNode: SelectSyntax) {
        let select = new Select();

        select = this.parseFromItems(selectNode, select);
        select = this.parseColumns(selectNode, select);
        select = this.parseWhere(selectNode, select);
        select = this.parseOrderBy(selectNode, select);
        select = this.parseLimit(selectNode, select);

        return select;
    }

    private parseFromItems(
        selectNode: SelectSyntax,
        select: Select
    ) {
        const fromItems = selectNode.row.from || [];
        fromItems.forEach(fromItem => {
            select = this.parseFromItem(select, fromItem as any)
        });

        return select;
    }

    private parseFromItem(
        select: Select,
        fromItem: FromTable
    ) {
        const alias = fromItem.row.as;
        const tableLink = fromItem.row.table as TableLink;
        const tableRef = parseFromTable(tableLink, alias);
        let fromTable = new From({source: tableRef});

        const joinsSyntaxes = fromItem.row.joins || [];
        joinsSyntaxes.forEach(joinSyntax =>  {
            fromTable = this.parseJoin(fromTable, joinSyntax)
        });
        
        select = select.addFrom(fromTable);

        return select;
    }

    private parseJoin(
        from: From,
        joinSyntax: JoinSyntax
    ) {
        const fromItem = joinSyntax.row.from as FromTable;
        const alias = fromItem.row.as;
        const tableLink = fromItem.row.table as TableLink;
        const type = joinSyntax.row.type as string;

        const tableRef = parseFromTable(tableLink, alias);

        const on = this.expressionParser.parse(
            (joinSyntax.row as any).on
        );
        
        const join = new Join(type, tableRef, on);
        
        from = from.addJoin(join);
        return from;
    }

    private parseColumns(
        selectSyntax: SelectSyntax,
        select: Select
    ) {
        const columns = selectSyntax.row.select;

        for (const column of columns) {
            const name = column.row.as!.toValue();
            const expressionSyntax = column.row.expression;

            const selectColumn = new SelectColumn({
                name,
                expression: this.expressionParser.parse(
                    expressionSyntax
                )
            });

            select = select.addColumn(selectColumn);
        }

        return select;
    }

    private parseWhere(
        selectSyntax: SelectSyntax,
        select: Select
    ) {
        if ( selectSyntax.row.where ) {
            const whereExpression = this.expressionParser.parse(
                selectSyntax.row.where
            );

            select = select.addWhere(whereExpression);
        }

        return select;
    }

    private parseOrderBy(
        selectSyntax: SelectSyntax,
        select: Select
    ) {
        const orderBySyntax = selectSyntax.row.orderBy || [];
        if ( orderBySyntax.length ) {
            const orderByItems: OrderByItem[] = orderBySyntax.map(orderItemSyntax => {
                const type = (orderItemSyntax.row.vector || "asc")
                    .toLowerCase() as "asc" | "desc";

                const orderItem = new OrderByItem({
                    type,
                    expression: this.expressionParser.parse(
                        orderItemSyntax.row.expression
                    ),
                    nulls: orderItemSyntax.row.nulls as any
                });
                return orderItem;
            });
            const orderBy = new OrderBy(orderByItems);

            select = select.addOrderBy(orderBy)
        }

        return select;
    }

    private parseLimit(
        selectSyntax: SelectSyntax,
        select: Select
    ) {
        const limit = +(selectSyntax.row.limit || 0);

        if ( limit > 0 ) {
            select = select.setLimit(limit);
        }

        return select;
    }
}