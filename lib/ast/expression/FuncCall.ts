// cycle import
import { Expression } from "./Expression";
import { TableReference } from "../TableReference";
import { Table } from "../Table";
import { UnknownExpressionElement } from "./UnknownExpressionElement";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { Spaces } from "../Spaces";

export class FuncCall extends AbstractExpressionElement {

    readonly name: string;
    readonly args: Expression[];
    readonly where?: Expression;
    readonly distinct: boolean;
    constructor(
        name: string,
        args: Expression[],
        where?: Expression,
        distinct?: boolean
    ) {
        super();
        this.name = name;
        this.args = args;
        this.where = where;
        this.distinct = distinct ? true : false;
    }

    protected children() {
        const children = this.args.slice();
        if ( this.where ) {
            children.push( this.where );
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
        replaceTable: TableReference | Table,
        toTable: TableReference
    ) {
        const newArgs = this.args.map(arg =>
            arg.replaceTable(replaceTable, toTable)
        );
        return this.clone(newArgs);
    }

    replaceColumn(replaceColumn: string, toSql: string) {
        const newArgs = this.args.map(arg =>
            arg.replaceColumn(replaceColumn, toSql)
        );
        return this.clone(newArgs);
    }

    replaceFuncCall(replaceFunc: FuncCall, toSql: string) {
        if ( replaceFunc.equal(this) ) {
            return UnknownExpressionElement.fromSql(toSql);
        }

        return this.clone();
    }

    clone(newArgs?: Expression[]) {
        return new FuncCall(
            this.name,
            newArgs || this.args.map(arg => arg.clone()),
            this.where ?
                this.where.clone() :
                undefined,
            this.distinct
        );
    }

    template(spaces: Spaces) {
        let sql = "";

        sql += `${this.name}(`;

        const isLongArgs = this.args.join(", ").trim().length > 25;
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

        sql += ")";

        if ( this.where ) {
            sql += " filter (where ";
            sql += this.where.toString();
            sql += ")";
        }

        return sql.split("\n");
    }
}
