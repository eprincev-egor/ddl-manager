import {
    Select as SelectSyntax,
    FunctionCall,
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
import { TableReference } from "../database/schema/TableReference";
import { strict } from "assert";
import { parseFromTable } from "./utils";

export class SelectParser {

    private expressionParser = new ExpressionParser();
    
    parse(cacheFor: TableReference, selectNode: SelectSyntax) {
        const selectData = selectNode.row;
        let select = new Select();

        if ( selectData.with ) {
            throw new Error("CTE (with queries) are not supported");
        }
        if ( selectData.union ) {
            throw new Error("UNION are not supported");
        }
        const hasSubQuery = selectNode.filterChildrenByInstance(SelectSyntax).length > 0;
        if ( hasSubQuery ) {
            throw new Error("SUB QUERIES are not supported");
        }
        if ( selectData.groupBy ) {
            throw new Error("GROUP BY are not supported");
        }

        const columns = selectData.select;
        if ( !columns || !columns.length ) {
            throw new Error("required select any columns or expressions");
        }

        for (let i = 0, n = columns.length; i < n; i++) {
            const columnNode = columns[i];
            const columnAlias = columnNode.row.as?.toValue();

            for (let j = i + 1; j < n; j++) {
                const nextColumn = columns[j];
                const nextColumnAlias = nextColumn.row.as?.toValue()

                if ( columnAlias === nextColumnAlias ) {
                    throw new Error(`duplicated cache column ${cacheFor.toString()}\.${columnAlias}`);
                }
            }

            const funcCalls = columnNode.filterChildrenByInstance(FunctionCall);
            for (const funcCall of funcCalls) {
                const name = String(funcCall.row.call);

                if ( name == "string_agg" ) {
                    const args = funcCall.row.arguments || [];

                    if ( args.length === 1 ) {
                        throw new Error(`required delimiter for string_agg, column: ${columnAlias}`);
                    }
                }
            }
        }


        select = this.parseFromItems(cacheFor, selectNode, select);
        select = this.parseColumns(cacheFor, selectNode, select);
        select = this.parseWhere(cacheFor, selectNode, select);
        select = this.parseOrderBy(cacheFor, selectNode, select);
        select = this.parseLimit(selectNode, select);

        return select;
    }

    private parseFromItems(
        cacheFor: TableReference,
        selectNode: SelectSyntax,
        select: Select
    ) {
        const fromItems = selectNode.row.from || [];
        fromItems.forEach(fromItem => {
            strict.ok(fromItem instanceof FromTable, "supported only from table");
            select = this.parseFromItem(cacheFor, select, fromItem)
        });

        return select;
    }

    private parseFromItem(
        cacheFor: TableReference,
        select: Select,
        fromItem: FromTable
    ) {
        const alias = fromItem.row.as;
        const tableLink = fromItem.row.table as TableLink;

        strict.ok(tableLink, "sub queries are not supported");

        const tableRef = parseFromTable(tableLink, alias);
        let fromTable = new From({source: tableRef});

        const joinsSyntaxes = fromItem.row.joins || [];
        joinsSyntaxes.forEach(joinSyntax =>  {
            fromTable = this.parseJoin(cacheFor, select, fromTable, joinSyntax)
        });
        
        select = select.addFrom(fromTable);

        return select;
    }

    private parseJoin(
        cacheFor: TableReference,
        select: Select,
        from: From,
        joinSyntax: JoinSyntax
    ) {
        const fromItem = joinSyntax.row.from as FromTable;
        const alias = fromItem.row.as;
        const tableLink = fromItem.row.table as TableLink;

        strict.ok(tableLink, "supported only 'join table'");

        const type = joinSyntax.row.type as string;

        const tableRef = parseFromTable(tableLink, alias);

        strict.ok("on" in joinSyntax.row, "supported only joins with on condition")

        const on = this.expressionParser.parse(
            select,
            [
                cacheFor, 
                from.source as TableReference, 
                ...from.joins.map(prevJoin =>
                    prevJoin.getTable()
                ),
                tableRef
            ],
            joinSyntax.row.on
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
        const columns = selectSyntax.row.select;

        for (const column of columns) {
            const nameSyntax = column.row.as;

            strict.ok(nameSyntax, "required alias for every cache column: " + column.toString());

            const name = nameSyntax.toString();

            const expressionSyntax = column.row.expression;

            const selectColumn = new SelectColumn({
                name,
                expression: this.expressionParser.parse(
                    select, 
                    [cacheFor], 
                    expressionSyntax
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
            const whereExpression = this.expressionParser.parse(
                select, [cacheFor],
                selectSyntax.row.where
            );

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
            const orderByItems: OrderByItem[] = orderBySyntax.map(orderItemSyntax => {
                const type = (orderItemSyntax.row.vector || "asc")
                    .toLowerCase() as "asc" | "desc";

                const orderItem = new OrderByItem({
                    type,
                    expression: this.expressionParser.parse(
                        select, [cacheFor],
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