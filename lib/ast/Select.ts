import { AbstractAstElement } from "./AbstractAstElement";
import { ColumnReference } from "./expression/ColumnReference";
import { Expression } from "./expression/Expression";
import { From } from "./From";
import { SelectColumn } from "./SelectColumn";
import { Spaces } from "./Spaces";
import { TableReference, IReferenceFilter } from "../database/schema/TableReference";
import { OrderBy } from "./OrderBy";
import { TableID } from "../database/schema/TableID";
import { strict } from "assert";
import { fixArraySearchForDifferentArrayTypes } from "../cache/trigger-builder/condition/fixArraySearchForDifferentArrayTypes";

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
        const clone = this.clone({
            columns: [
                ...this.columns.map(column => column.clone()),
                newColumn
            ]
        });
        return clone;
    }

    addFrom(newFrom: From): Select {
        const clone = this.clone({
            from: [
                ...this.from.map(from => from.clone()),
                newFrom
            ]
        });
        return clone;
    }

    addWhere(newWhere: Expression) {
        const clone = this.clone({
            where: newWhere
        });
        return clone;
    }

    addOrderBy(orderBy: OrderBy) {
        const clone = this.clone({
            orderBy
        });
        return clone;
    }

    setLimit(limit: number) {
        const clone = this.clone({
            limit
        });
        return clone;
    }

    clone(params: Partial<ISelectParams> = {}): Select {
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

    hasArraySearchOperator() {
        return this.where?.hasArraySearchOperator();
    }

    fixArraySearchForDifferentArrayTypes(fromTable?: TableReference) {
        if ( this.where?.hasArraySearchOperator() ) {
            return this.clone({
                where: fixArraySearchForDifferentArrayTypes(
                    fromTable ?? this.getFromTable(),
                    this.where
                )
            });
        }

        return this;
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
        return this.clone({
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

            ...this.printColumns(spaces),

            ...this.printFrom(spaces),

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

    private printColumns(spaces: Spaces) {
        if ( this.columns.length === 0 ) {
            return [];
        }

        const nextSpaces = spaces.plusOneLevel();
        const output = this.columns[0].template(nextSpaces);
        
        for (const column of this.columns.slice(1)) {
            output[ output.length - 1 ] = output[ output.length - 1 ] + ",";

            output.push(...column.template(nextSpaces));
        }

        return output;
    }

    private printFrom(spaces: Spaces) {
        if ( this.from.length === 0 ) {
            return [];
        }

        const output = [
            ...this.from[0].template(spaces)
        ];
        output[0] = `${spaces}from ${output[0].trim()}`;

        for (const from of this.from.slice(1)) {
            output.push(spaces + ",");
            output.push(...from.template(
                spaces.plusOneLevel()
            ));
        }

        return output;
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
            if ( fromItem.source instanceof TableReference ) {
                allReferences.push( fromItem.source );
            }

            for (const join of fromItem.joins) {
                if ( join.table instanceof TableReference ) {
                    allReferences.push( join.table );
                }
            }
        }

        return allReferences;
    }

    getFromTableId() {
        return this.getFromTable().table
    }

    getFromTable(): TableReference {
        strict.equal(this.from.length, 1, "expected only one from");
        strict.ok(this.from[0].source instanceof TableReference, "expected from table");

        return this.from[0].source;
    }
    
    getDeterministicOrderBy() {
        const orderBy = this.orderBy;
        if ( !orderBy || orderBy.hasIdSort() ) {
            return orderBy;
        }

        const from = this.getFromTable();
        return orderBy.addIdSort(from);
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