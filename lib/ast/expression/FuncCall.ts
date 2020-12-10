// cycle import
import { Expression } from "./Expression";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { UnknownExpressionElement } from "./UnknownExpressionElement";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { Spaces } from "../Spaces";

// https://postgrespro.ru/docs/postgrespro/10/queries-order
export interface IOrderByItem {
    vector: "asc" | "desc";
    expression: Expression;
    using?: string;
    nulls: "first" | "last";
}

export class FuncCall extends AbstractExpressionElement {

    readonly name: string;
    readonly args: Expression[];
    readonly where?: Expression;
    readonly distinct: boolean;
    readonly orderBy: IOrderByItem[];
    constructor(
        name: string,
        args: Expression[],
        where?: Expression,
        distinct?: boolean,
        orderBy: Partial<IOrderByItem>[] = []
    ) {
        super();
        this.name = name;
        this.args = args;
        this.where = where;
        this.distinct = distinct ? true : false;
        this.orderBy = this.prepareOrderBy(orderBy);
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
        replaceTable: TableReference | TableID,
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
            this.distinct,
            this.cloneOrderBy()
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

        if ( this.orderBy.length ) {
            sql += " " + this.toStringOrderBy() + " ";
        }

        sql += ")";

        if ( this.where ) {
            sql += " filter (where ";
            sql += this.where.toString();
            sql += ")";
        }

        return sql.split("\n");
    }

    // TODO: new abstraction
    private prepareOrderBy(orderBy: Partial<IOrderByItem>[]): IOrderByItem[] {
        const preparedOrderBy = orderBy.map(item => {
            if ( !item.expression ) {
                throw new Error("required orderBy expression")
            }

            const vector = item.vector || "asc";
            const preparedItem = {
                vector,
                expression: item.expression,
                using: item.using,
                // https://postgrespro.ru/docs/postgrespro/10/queries-order
                // По умолчанию значения NULL считаются больше любых других, 
                nulls: (
                    item.nulls || (
                        // то есть подразумевается NULLS FIRST для порядка DESC и 
                        vector === "desc" ? 
                            "first" :
                        // NULLS LAST в противном случае.
                            "last"
                    )
                )
            };
            return preparedItem;
        });
        return preparedOrderBy;
    }

    private cloneOrderBy() {
        const clone = this.orderBy.map(item => ({
            ...item,
            expression: item.expression.clone()
        }));
        return clone;
    }

    private toStringOrderBy() {
        return "order by " + this.orderBy.map(item => 
            `${item.expression} ${item.vector} nulls ${ item.nulls }`
        ).join(", ");
    }
}
