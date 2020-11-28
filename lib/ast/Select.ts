import { AbstractAstElement } from "./AbstractAstElement";
import { ColumnReference } from "./expression/ColumnReference";
import { Expression } from "./expression/Expression";
import { From } from "./From";
import { SelectColumn } from "./SelectColumn";
import { Spaces } from "./Spaces";
import { TableReference, IReferenceFilter } from "./TableReference";

interface ISelectParams {
    columns: SelectColumn[];
    where?: Expression;
    from: From[];
    intoRow?: string;
}

export class Select extends AbstractAstElement {
    readonly columns!: SelectColumn[];
    readonly where?: Expression;
    readonly from!: From[];
    readonly intoRow?: string;

    constructor(params: ISelectParams = {
        columns: [], 
        from: []
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

    cloneWith(params: Partial<ISelectParams>) {
        const clone = new Select({
            columns: this.columns.map(column => column.clone()),
            from: this.from.map(from => from.clone()),
            where: this.where ? this.where.clone() : undefined,
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