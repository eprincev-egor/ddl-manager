import { AbstractAstElement } from "./AbstractAstElement";
import { ColumnReference } from "./expression/ColumnReference";
import { Expression } from "./expression/Expression";
import { From } from "./From";
import { SelectColumn } from "./SelectColumn";
import { Spaces } from "./Spaces";
import { TableReference, IReferenceFilter } from "../database/schema/TableReference";

export interface OrderByItem {
    expression: Expression;
    type: "asc" | "desc";
    nulls: "first" | "last";
}

interface ISelectParams {
    columns: SelectColumn[];
    where?: Expression;
    from: From[];
    intoRow?: string;
    orderBy: OrderByItem[];
    limit?: number;
}

export class Select extends AbstractAstElement {
    readonly columns!: SelectColumn[];
    readonly where?: Expression;
    readonly from!: From[];
    readonly orderBy!: OrderByItem[];
    readonly intoRow?: string;
    readonly limit?: number;

    constructor(params: ISelectParams = {
        columns: [], 
        from: [],
        orderBy: []
    }) {
        super();
        Object.assign(this, params);
    }

    addColumn(newColumn: SelectColumn): Select {
        const clone = this.cloneWith({
            columns: [
                ...this.columns.map(column => column.clone()),
                newColumn
            ]
        });
        return clone;
    }

    addFrom(newFrom: From): Select {
        const clone = this.cloneWith({
            from: [
                ...this.from.map(from => from.clone()),
                newFrom
            ]
        });
        return clone;
    }

    addWhere(newWhere: Expression) {
        const clone = this.cloneWith({
            where: newWhere
        });
        return clone;
    }

    addOrderBy(orderBy: OrderByItem[]) {
        const clone = this.cloneWith({
            orderBy
        });
        return clone;
    }

    setLimit(limit: number) {
        const clone = this.cloneWith({
            limit
        });
        return clone;
    }

    cloneWith(params: Partial<ISelectParams>) {
        const clone = new Select({
            columns: this.columns.map(column => column.clone()),
            from: this.from.map(from => from.clone()),
            where: this.where ? this.where.clone() : undefined,
            orderBy: this.orderBy.map(orderItem => ({
                expression: orderItem.expression.clone(),
                type: orderItem.type,
                nulls: orderItem.nulls
            })),
            limit: this.limit,
            ...params
        });
        return clone;
    }

    findTableReference(filter: IReferenceFilter) {
        const outputTableRef = this.getAllTableReferences().find(tableRef => 
            tableRef.matched(filter)
        );
        return outputTableRef;
    }

    template(spaces: Spaces) {
        return [
            spaces + "select",

            this.columns.map(column =>
                column.toSQL( spaces )
            ).join(",\n"),

            ...(this.from.length ? [
                spaces + "from " + this.from.join(", "),
            ]: []),

            ...(this.where ? [
                spaces + "where",
                this.where.toSQL( spaces.plusOneLevel() )
            ]: []),

            ...(this.orderBy.length ? [
                spaces + "order by",
                this.orderBy.map(orderItem =>
                    orderItem.expression.toSQL( spaces.plusOneLevel() ) +
                    " " + orderItem.type +
                    " nulls " + orderItem.nulls
                ).join(",\n")
            ]: []),

            ...(this.limit ? [
                spaces + `limit ${this.limit}`
            ]: []),

            ...(this.intoRow ? [
                spaces + `into ${ this.intoRow };`
            ]: [])
        ];
    }

    getAllColumnReferences() {
        const allReferences: ColumnReference[] = [];

        for (const column of this.columns) {
            allReferences.push( ...column.expression.getColumnReferences() );
        }

        for (const fromItem of this.from) {
            for (const join of fromItem.joins) {
                allReferences.push( ...join.on.getColumnReferences() );
            }
        }

        if ( this.where ) {
            allReferences.push( ...this.where.getColumnReferences() );
        }

        for (const orderItem of this.orderBy) {
            allReferences.push( ...orderItem.expression.getColumnReferences() );
        }

        return allReferences;
    }

    getAllTableReferences() {
        const allReferences: TableReference[] = [];

        for (const fromItem of this.from) {
            allReferences.push( fromItem.table );

            for (const join of fromItem.joins) {
                allReferences.push( join.table );
            }
        }

        return allReferences;
    }
}