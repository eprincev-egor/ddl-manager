import { Cache, ColumnReference } from "../../ast";

export interface IDependencies {
    [table: string]: {
        columns: string[]
    };
}

export function findDependencies(cache: Cache) {
    const dependencies: IDependencies = {
        [cache.for.table.toString()]: {columns: []}
    };

    for (const tableRef of cache.select.getAllTableReferences()) {
        dependencies[ tableRef.table.toString() ] = {columns: []};
    }

    for (const columnRef of cache.select.getAllColumnReferences()) {
        addColumnReference(dependencies, columnRef);
    }

    return dependencies;
}

function addColumnReference(
    dependencies: IDependencies,
    columnRef: ColumnReference
) {
    const tableDependencies = getOrCreateTableDependencies(
        dependencies,
        columnRef
    );

    if ( !tableDependencies.columns.includes(columnRef.name) ) {
        tableDependencies.columns.push(columnRef.name);
        tableDependencies.columns.sort();
    }
}

function getOrCreateTableDependencies(
    dependencies: IDependencies,
    columnRef: ColumnReference
) {
    const table = columnRef.tableReference.table.toString();

    const tableDependencies = dependencies[ table ];
    if ( tableDependencies ) {
        return tableDependencies;
    }

    dependencies[ table ] = {columns: []};
    return dependencies[ table ];
}