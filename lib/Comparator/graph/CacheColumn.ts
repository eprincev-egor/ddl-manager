import { Select, Expression, SelectColumn } from "../../ast";
import { TableReference } from "../../database/schema/TableReference";
import { uniqBy } from "lodash";

export interface CacheColumnParams {
    for: TableReference;
    name: string;
    cache: CacheId;
    select: Select;
}

export interface CacheId {
    name: string;
    signature: string;
}

export class CacheColumn {
    for: TableReference;
    name: string;
    cache: CacheId;
    select: Select;
    /** which columns need for that column? */
    dependencies: CacheColumn[];
    /** which columns use this columns? */
    usedInColumns: CacheColumn[];

    constructor(params: CacheColumnParams) {
        this.for = params.for;
        this.name = params.name;
        this.cache = params.cache;
        this.select = params.select;
        this.dependencies = [];
        this.usedInColumns = [];
    }

    isRoot() {
        return (
            this.findCircularUses().length >= this.dependencies.length
        );
    }

    findCircularUses() {
        // TODO: many levels of circular dependency
        return this.usedInColumns.filter(dependencyColumn =>
            this.dependencies.includes(dependencyColumn)
        );
    }

    findNotCircularUses() {
        // TODO: many levels of circular dependency
        return this.usedInColumns.filter(dependencyColumn =>
            !this.dependencies.includes(dependencyColumn)
        );
    }

    getColumnRefs() {
        return this.select.getAllColumnReferences();
    }

    hasForeignTablesDeps() {
        return this.getColumnRefs().some(columnRef =>
            !columnRef.tableReference.equal( this.for )
        );
    }

    getId() {
        return `${this.getTableId()}.${this.name}`;
    }

    getTableId() {
        return this.for.table.toString();
    }

    assignDependencies(dependencyColumns: CacheColumn[]) {
        // assign uses
        for (const dependencyColumn of dependencyColumns) {
            dependencyColumn.usedInColumns.push(this);
            dependencyColumn.usedInColumns = uniqBy(
                dependencyColumn.usedInColumns,
                (column) => column.getId()
            );
        }

        // assign self dependencies
        this.dependencies.push(...dependencyColumns);
        this.dependencies = uniqBy(this.dependencies, (column) => column.getId());
    }

    replaceExpression(newExpression: Expression) {
        return new CacheColumn({
            for: this.for,
            name: this.name,
            cache: this.cache,
            select: this.select.clone({
                columns: [new SelectColumn({
                    name: this.name,
                    expression: newExpression
                })]
            })
        });
    }
}