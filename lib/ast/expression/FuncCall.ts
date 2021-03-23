// cycle import
import { Expression } from "./Expression";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { UnknownExpressionElement } from "./UnknownExpressionElement";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { Spaces } from "../Spaces";
import { ColumnReference } from "./ColumnReference";
import { IExpressionElement } from "./interface";
import { OrderBy } from "../OrderBy";
import { OrderByItem } from "../OrderByItem";

export class FuncCall extends AbstractExpressionElement {

    readonly name: string;
    readonly args: Expression[];
    readonly where?: Expression;
    readonly distinct: boolean;
    readonly orderBy?: OrderBy;
    constructor(
        name: string,
        args: Expression[],
        where?: Expression,
        distinct?: boolean,
        orderBy?: OrderBy
    ) {
        super();
        this.name = name;
        this.args = args;
        this.where = where;
        this.distinct = distinct ? true : false;
        this.orderBy = orderBy;
    }

    protected children() {
        const children = this.args.slice();

        if ( this.where ) {
            children.push( this.where );
        }

        if ( this.orderBy ) {
            this.orderBy.items.forEach(item => {
                children.push( item.expression );
            });
        }
        
        return children;
    }

    getFuncCalls() {
        return [
            this,
            ...super.getFuncCalls()
        ];
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        const newArgs = this.args.map(arg =>
            arg.replaceTable(replaceTable, toTable)
        );

        let orderBy: OrderBy | undefined;
        if ( this.orderBy ) {
            const orderByItems = this.orderBy.items.map(item => 
                new OrderByItem({
                    ...item,
                    expression: item.expression.replaceTable(replaceTable, toTable)
                })
            );
            orderBy = new OrderBy(orderByItems);
        }

        let newWhere = this.where;
        if ( newWhere ) {
            newWhere = newWhere.replaceTable(replaceTable, toTable);
        }

        return this.clone(
            newArgs,
            orderBy,
            newWhere
        );
    }

    replaceColumn(replaceColumn: ColumnReference, toSql: IExpressionElement) {
        const newArgs = this.args.map(arg =>
            arg.replaceColumn(replaceColumn, toSql)
        );

        let orderBy: OrderBy | undefined;
        if ( this.orderBy ) {
            const orderByItems = this.orderBy.items.map(item => 
                new OrderByItem({
                    ...item,
                    expression: item.expression.replaceColumn(replaceColumn, toSql)
                })
            );
            orderBy = new OrderBy(orderByItems);
        }

        let newWhere = this.where;
        if ( newWhere ) {
            newWhere = newWhere.replaceColumn(replaceColumn, toSql);
        }

        return this.clone(
            newArgs,
            orderBy
        );
    }

    replaceFuncCall(replaceFunc: FuncCall, toSql: string) {
        if ( replaceFunc.equal(this) ) {
            return UnknownExpressionElement.fromSql(toSql);
        }

        return this.clone();
    }

    clone(
        newArgs?: Expression[],
        newOrderBy?: OrderBy,
        newWhere?: Expression
    ) {
        return new FuncCall(
            this.name,
            newArgs || this.args.map(arg => arg.clone()),
            newWhere || (this.where ?
                this.where.clone() :
                undefined),
            this.distinct,
            newOrderBy || this.cloneOrderBy()
        );
    }

    withoutWhere() {
        return new FuncCall(
            this.name,
            this.args.map(arg => arg.clone()),
            undefined,
            this.distinct,
            this.cloneOrderBy()
        );
    }

    template(spaces: Spaces) {
        let sql = "";

        sql += `${this.name}(`;

        if ( this.distinct ) {
            sql += "distinct ";
        }

        const isLongArgs = (
            this.orderBy ||
            this.args.join(", ").trim().length > 24
        );
        if ( isLongArgs ) {
            sql += "\n";

            for (let i = 0, n = this.args.length; i < n; i++) {
                if ( i > 0 ) {
                    sql += ",\n";
                }

                const arg = this.args[i];
                sql += arg.toSQL( spaces.plusOneLevel() );
            }

            sql += "\n";
            sql += spaces;
        }
        else {
            sql += this.args.join(", ");
        }

        if ( this.orderBy ) {
            sql += this.orderBy.toSQL( spaces.plusOneLevel() ) + "\n";
        }

        sql += ")";

        if ( this.where ) {
            sql += " filter (where ";
            sql += this.where.toString();
            sql += ")";
        }

        return sql.split("\n");
    }

    private cloneOrderBy() {
        if ( this.orderBy ) {
            return this.orderBy.clone();
        }
    }
}
