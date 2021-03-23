import { 
    Select as SelectSyntax, 
    Column,
    Expression as ExpressionSyntax,
    FromItem,
    Join as JoinSyntax,
    TableLink,
    ObjectName
} from "grapeql-lang";
import { ExpressionParser } from "./ExpressionParser";
import { TableReferenceParser } from "./TableReferenceParser";
import {
    From, Join, Select,
    SelectColumn, OrderByItem 
} from "../ast";
import { TableReference } from "../database/schema/TableReference";
import assert from "assert";

export class SelectParser {

    private expressionParser = new ExpressionParser();
    private tableParser = new TableReferenceParser();
    
    parse(cacheFor: TableReference, selectSyntax: SelectSyntax) {

        let select = new Select();

        if ( selectSyntax.get("with") ) {
            throw new Error("CTE (with queries) are not supported");
        }
        if ( selectSyntax.get("union") ) {
            throw new Error("UNION are not supported");
        }
        const hasSubQuery = selectSyntax.filterChildrenByInstance(SelectSyntax).length > 0;
        if ( hasSubQuery ) {
            throw new Error("SUB QUERIES are not supported");
        }
        if ( selectSyntax.get("groupBy") ) {
            throw new Error("GROUP BY are not supported");
        }

        const columns = selectSyntax.get("columns");
        if ( !columns || !columns.length ) {
            throw new Error("required select any columns or expressions");
        }

        for (let i = 0, n = columns.length; i < n; i++) {
            const column = columns[i];
            const columnAlias = (column.get("as") || "").toString();

            for (let j = i + 1; j < n; j++) {
                const nextColumn = columns[j];
                const nextColumnAlias = (nextColumn.get("as") || "").toString();

                if ( columnAlias === nextColumnAlias ) {
                    throw new Error(`duplicated cache column ${cacheFor.toString()}\.${columnAlias}`);
                }
            }
        }


        select = this.parseFromItems(cacheFor, selectSyntax, select);
        select = this.parseColumns(cacheFor, selectSyntax, select);
        select = this.parseWhere(cacheFor, selectSyntax, select);
        select = this.parseOrderBy(cacheFor, selectSyntax, select);
        select = this.parseLimit(cacheFor, selectSyntax, select);

        return select;
    }

    private parseFromItems(
        cacheFor: TableReference,
        selectSyntax: SelectSyntax,
        select: Select
    ) {
        const fromItems = selectSyntax.get("from") || [];
        fromItems.forEach(fromItem => {
            select = this.parseFromItem(cacheFor, select, fromItem)
        });

        return select;
    }

    private parseFromItem(
        cacheFor: TableReference,
        select: Select,
        fromItem: FromItem
    ) {
        const alias = fromItem.get("as");
        const tableLink = fromItem.get("table") as TableLink;

        assert.ok(tableLink, "sub queries are not supported");

        const tableRef = this.tableParser.parse(tableLink, alias);
        let fromTable = new From(tableRef);

        const joinsSyntaxes = fromItem.get("joins") || [];
        joinsSyntaxes.forEach(joinSyntax =>  {
            fromTable = this.parseJoin(cacheFor, select, fromTable, joinSyntax)
        });
        
        select = select.addFrom(fromTable);

        return select;
    }

    private parseJoin(cacheFor: TableReference, select: Select, from: From, joinSyntax: JoinSyntax) {
        const fromItem = joinSyntax.get("from") as FromItem;
        const alias = fromItem.get("as");
        const tableLink = fromItem.get("table") as TableLink;

        assert.ok(tableLink, "supported only 'join table'");

        const type = joinSyntax.get("type") as string;

        const tableRef = this.tableParser.parse(tableLink, alias);

        const onSql = (joinSyntax.get("on") as ExpressionSyntax).toString();
        const on = this.expressionParser.parse(
            select,
            [
                cacheFor, 
                from.table, 
                ...from.joins.map(prevJoin =>
                    prevJoin.table
                ),
                tableRef
            ],
            onSql 
        );
        
        const join = new Join(type, tableRef, on);
        
        from = from.addJoin(join);
        return from;
    }

    private parseColumns(
        cacheFor: TableReference,
        selectSyntax: SelectSyntax,
        select: Select
    ) {
        const columns = selectSyntax.get("columns") as Column[];

        for (const column of columns) {
            const nameSyntax = column.get("as") as ObjectName;

            assert.ok(nameSyntax, "required alias for every cache column: " + column.toString());

            const name = nameSyntax.toLowerCase() as string;

            const expressionSyntax = column.get("expression") as ExpressionSyntax;

            const selectColumn = new SelectColumn({
                name,
                expression: this.expressionParser.parse(
                    select, 
                    [cacheFor], 
                    expressionSyntax.toString()
                )
            });

            select = select.addColumn(selectColumn);
        }

        return select;
    }

    private parseWhere(
        cacheFor: TableReference,
        selectSyntax: SelectSyntax,
        select: Select
    ) {
        if ( selectSyntax.row.where ) {
            const whereSql = selectSyntax.row.where.toString();
            const whereExpression = this.expressionParser.parse(select, [cacheFor], whereSql);

            select = select.addWhere(whereExpression);
        }

        return select;
    }

    private parseOrderBy(
        cacheFor: TableReference,
        selectSyntax: SelectSyntax,
        select: Select
    ) {
        const orderBySyntax = selectSyntax.row.orderBy || [];
        if ( orderBySyntax.length ) {
            const orderBy: OrderByItem[] = orderBySyntax.map(orderItemSyntax => {
                const itemExpressionSql = orderItemSyntax.row.expression!.toString();
                const type = (orderItemSyntax.row.vector || "asc")
                    .toLowerCase() as "asc" | "desc";

                const nulls = (orderItemSyntax.row.nulls || "")
                    .toLowerCase() as "first" | "last" || "";

                const orderItem: OrderByItem = {
                    type,
                    expression: this.expressionParser.parse(
                        select, [cacheFor], itemExpressionSql
                    ),
                    nulls: nulls || "first"
                };
                return orderItem;
            });
            select = select.addOrderBy(orderBy)
        }

        return select;
    }

    private parseLimit(
        cacheFor: TableReference,
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