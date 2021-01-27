import { Cache, ColumnReference } from "../../ast";

export interface IDependencies {
    [table: string]: ITableDependencies;
}

export interface ITableDependencies {
    columns: string[];
}

// TODO: split to two functions and fix tests
export function findDependencies(cache: Cache, addCacheRefDeps = true) {
    const dependencies: IDependencies = {
        [cache.for.table.toString()]: {columns: []}
    };

    for (const tableRef of cache.select.getAllTableReferences()) {
        dependencies[ tableRef.table.toString() ] = {columns: []};
    }

    for (const columnRef of cache.select.getAllColumnReferences()) {
        if ( !addCacheRefDeps && columnRef.tableReference.equal(cache.for) ) {
            continue;
        }
        addColumnReference(dependencies, columnRef);
    }

    return dependencies;
}

export function findDependenciesToCacheTable(cache: Cache): ITableDependencies {
    const tableDependencies: ITableDependencies = {columns: []};

    for (const columnRef of cache.select.getAllColumnReferences()) {
        if ( !columnRef.tableReference.equal(cache.for) ) {
            continue;
        }

        tableDependencies.columns.push(columnRef.name);
        tableDependencies.columns.sort();
    }

    return tableDependencies;
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