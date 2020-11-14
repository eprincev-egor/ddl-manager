import { 
    Select as SelectSyntax, 
    Column,
    Expression as ExpressionSyntax,
    FromItem,
    Join as JoinSyntax
} from "grapeql-lang";
import { ExpressionParser } from "./ExpressionParser";
import { TableReferenceParser } from "./TableReferenceParser";
import { From, Join, Select, SelectColumn, TableReference } from "../ast";
import assert from "assert";

export class SelectParser {

    private expressionParser = new ExpressionParser();
    private tableParser = new TableReferenceParser();
    
    parse(cacheFor: TableReference, selectSyntax: SelectSyntax) {

        let select = new Select();

        select = this.parseFromItems(cacheFor, selectSyntax, select);
        select = this.parseColumns(cacheFor, selectSyntax, select);
        select = this.parseWhere(cacheFor, selectSyntax, select);

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
        const tableLink = fromItem.get("table");

        assert.ok(tableLink, "supported only 'from table'");

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
        const tableLink = fromItem.get("table");

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
            const nameSyntax = column.get("as");

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
}