import { AbstractAstElement } from "./AbstractAstElement";
import { ColumnReference } from "./expression/ColumnReference";
import { Expression } from "./expression/Expression";
import { From } from "./From";
import { SelectColumn } from "./SelectColumn";
import { Spaces } from "./Spaces";
import { TableReference, IReferenceFilter } from "../database/schema/TableReference";
import { OrderBy } from "./OrderBy";
import { TableID } from "../database/schema/TableID";

interface ISelectParams {
    columns: SelectColumn[];
    where?: Expression;
    from: From[];
    intoRow?: string;
    orderBy?: OrderBy;
    limit?: number;
    forUpdate?: boolean;
}

export class Select extends AbstractAstElement {
    readonly columns!: SelectColumn[];
    readonly where?: Expression;
    readonly from!: From[];
    readonly orderBy?: OrderBy;
    readonly intoRow?: string;
    readonly limit?: number;
    readonly forUpdate: boolean;

    constructor(params: ISelectParams = {
        columns: [], 
        from: []
    }) {
        super();
        Object.assign(this, params);

        if ( params.orderBy ) {
            this.orderBy = params.orderBy;
        }

        this.forUpdate = !!params.forUpdate;
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

    addOrderBy(orderBy: OrderBy) {
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
            orderBy: this.orderBy ? this.orderBy.clone() : undefined,
            limit: this.limit,
            forUpdate: "forUpdate" in params ? params.forUpdate : this.forUpdate,
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

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return this.cloneWith({
            columns: this.columns.map(column => 
                column.replaceTable(replaceTable, toTable)
            ),
            from: this.from.map(from =>
                from.replaceTable(replaceTable, toTable)
            ),
            where: (
                this.where ? 
                    this.where.replaceTable(replaceTable, toTable) : 
                    undefined
            ),
            orderBy: (
                this.orderBy ? 
                    this.orderBy.replaceTable(replaceTable, toTable) : 
                    undefined
            ),
        })
    }

    equalSource(select: Select) {
        if ( this.where && select.where ) {
            this.where.equal(select.where)
        }

        if ( this.orderBy && select.orderBy ) {
            this.orderBy.equal(select.orderBy)
        }
        return (
            this.from.length === select.from.length &&
            this.from.every((fromItem, i) =>
                fromItem.equal(select.from[i])
            )
            &&
            equal(this.where, select.where)
            &&
            equal(this.orderBy, select.orderBy)
            &&
            this.limit === select.limit
        );
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

            ...(this.orderBy ?
                [this.orderBy.toSQL(spaces)] :
                []
            ),

            ...(this.limit ? [
                spaces + `limit ${this.limit}`
            ]: []),

            ...(this.forUpdate ? [
                spaces + "for update" + (this.intoRow ? "" : ";")
            ] : []),

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

        if ( this.orderBy ) {
            allReferences.push( ...this.orderBy.getColumnReferences() );
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

interface EqualItem {
    equal(item: this): boolean;
}

function equal<T extends EqualItem>(
    a: T | undefined,
    b: T | undefined
) {
    if ( a && b ) {
        return a.equal(b);
    }
    if ( !a && !b ) {
        return true;
    }
    return false;
}