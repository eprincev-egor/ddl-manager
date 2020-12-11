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

    const columnsRefsFromCountArgsOrOrderBy: ColumnReference[] = [];

    cache.select.columns.forEach(selectColumn => {
        const countCalls = selectColumn.getAggregations().filter(funCall =>
            funCall.name === "count"
        );

        countCalls.forEach(countCall => {

            if ( !countCall.distinct ) {
                countCall.args.forEach(arg => {
                    columnsRefsFromCountArgsOrOrderBy.push(
                        ...arg.getColumnReferences()
                    );
                });
            }
            
            
            countCall.orderBy.forEach(countOrderBy => {
                columnsRefsFromCountArgsOrOrderBy.push(
                    ...countOrderBy.expression.getColumnReferences()
                );
            })
        });
    });

    for (const columnRef of cache.select.getAllColumnReferences()) {
        const fromCountArgsOrOrderBy = columnsRefsFromCountArgsOrOrderBy
            .some(columnRefFromCount =>
                columnRefFromCount === columnRef
            );

        if ( !fromCountArgsOrOrderBy ) {
            addColumnReference(dependencies, columnRef);
        }
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