import { CacheColumn, CacheColumnParams } from "./CacheColumn";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { flatMap, uniqBy } from "lodash";
import { CacheUpdate } from "./CacheUpdate";
import { buildDependencyMatrix, groupByTables } from "./utils";
import { equalColumnName } from "../../database/schema/Column";
import { Cache, ColumnReference, Expression, Operator, Select } from "../../ast";
import { buildReferenceMeta } from "../../cache/processor/buildReferenceMeta";

export class CacheColumnGraph {

    static build(
        aggregators: string[],
        allCache: Cache[]
    ) {
        const cacheColumns: CacheColumnParams[] = [];

        for (const cache of allCache) {
            const selectForUpdate = cache.createSelectForUpdate(aggregators);

            for (const updateColumn of selectForUpdate.columns) {
                cacheColumns.push({
                    for: cache.for,
                    name: updateColumn.name,
                    cache: {
                        name: cache.name,
                        signature: cache.getSignature()
                    },
                    select: selectForUpdate.clone({
                        columns: [
                            updateColumn
                        ]
                    })
                });
            }

            const needLastRowColumn = (
                cache.select.from.length === 1 &&
                cache.select.orderBy &&
                cache.select.limit === 1
            );
            if ( needLastRowColumn ) {
                const fromRef = cache.select.getFromTable();
                const prevRef = new TableReference(
                    fromRef.table,
                    `prev_${ fromRef.table.name }`
                );
                const orderBy = cache.select.orderBy!.items[0]!;
                const columnName = cache.getIsLastColumnName();
                const referenceMeta = buildReferenceMeta(
                    cache, fromRef.table
                );

                cacheColumns.push({
                    for: fromRef,
                    name: columnName,
                    cache: {
                        name: cache.name,
                        signature: cache.getSignature()
                    },
                    select: Select.notExists(prevRef, Expression.and([
                        ...referenceMeta.columns.map(column =>
                            new Expression([
                                new ColumnReference(prevRef, column),
                                new Operator("="),
                                new ColumnReference(fromRef, column),
                            ])
                        ),
                        ...referenceMeta.filters.map(filter =>
                            filter.replaceTable(
                                fromRef,
                                prevRef
                            )
                        ),
                        new Expression([
                            new ColumnReference(prevRef, "id"),
                            new Operator(
                                orderBy.type === "desc" ? 
                                    ">" : "<"
                            ),
                            new ColumnReference(fromRef, "id"),
                        ])
                    ]), columnName)
                });
            }
        }

        const graph = new CacheColumnGraph(cacheColumns);
        return graph;
    }

    private allColumns: CacheColumn[];
    private tables: Record<string, CacheColumn[]>;
    private rootColumns: CacheColumn[];
    private dependencyMatrix: CacheColumn[][];

    constructor(allColumns: CacheColumnParams[]) {
        this.allColumns = allColumns
            .map(columnParams => new CacheColumn(columnParams));

        this.tables = groupByTables(this.allColumns);

        this.rootColumns = [];
        this.assignAllDependencies();

        this.dependencyMatrix = buildDependencyMatrix(
            this.rootColumns.slice()
        );
    }

    getAllColumns() {
        return this.allColumns;
    }

    getAllColumnsFromRootToDeps() {
        return flatMap(this.dependencyMatrix);
    }

    generateUpdatesFor(changedColumns: CacheColumn[]): CacheUpdate[] {
        const changesMap = buildChangesMap(changedColumns);

        const matrix = this.dependencyMatrix.map(columns =>
            columns.filter(column => 
                column.getId() in changesMap
            )
        );

        const updatesMatrix: CacheUpdate[][] = matrix.map(columns => 
            CacheUpdate.fromManyTables(columns)
        );
        const allUpdates = flatMap(updatesMatrix);
        return allUpdates;
    }

    generateAllUpdates(): CacheUpdate[] {
        const updatesMatrix: CacheUpdate[][] = this.dependencyMatrix
            .map(columns => CacheUpdate.fromManyTables(columns));

        const allUpdates = flatMap(updatesMatrix);
        return allUpdates;
    }

    getColumn(tableRef: TableReference | TableID | string, columnName: string) {
        return this.getColumns(tableRef).find(cacheColumn => 
            equalColumnName(cacheColumn.name, columnName)
        );
    }

    getColumns(tableRef: TableReference | TableID | string) {
        if ( typeof tableRef === "string" ) {
            tableRef = TableID.fromString(tableRef);
        }

        const tableId = (
            tableRef instanceof TableReference ? 
                tableRef.table : tableRef
        );
        return this.tables[ tableId.toString() ] || [];
    }

    getDependencyLevel(column: CacheColumn) {
        return this.dependencyMatrix.findIndex(level =>
            level.some(levelColumn => 
                levelColumn.name === column.name &&
                levelColumn.for.table.equal(column.for.table)
            )
        );
    }

    findCacheColumnsForTablesOrColumns(
        targetTablesOrColumns?: string | string[]
    ) {
        if ( !targetTablesOrColumns ) {
            return this.getAllColumns();
        }

        const concreteColumns: CacheColumn[] = [];
    
        for (const tableOrColumn of String(targetTablesOrColumns).split(/\s*,\s*/) ) {
            const path = tableOrColumn.trim().toLowerCase().split(".");
            const tableId = path.slice(0, 2).join(".");
            const tableColumns = this.getColumns(tableId);

            if ( path.length === 3 ) {
                const columnName = path.slice(-1)[0];
                const column = tableColumns.find(cacheColumn => 
                    cacheColumn.name === columnName
                );
                if ( column ) {
                    concreteColumns.push(column);
                }
            }
            else {
                concreteColumns.push(...tableColumns);
            }
        }

        return concreteColumns;
    }

    findCacheColumnsDependentOn(table: TableID): CacheColumn[] {
        const dependencyColumns: CacheColumn[] = [];

        for (const column of this.getAllColumns()) {
            const isDependency = column.select.getAllColumnReferences().some(depRef =>
                depRef.isFromTable(table)
            );
            if ( isDependency || column.for.table.equal(table) ) {
                dependencyColumns.push(column);
            }
        }

        return dependencyColumns;
    }

    private assignAllDependencies() {
        const allColumns = flatMap(Object.values(this.tables));
        for (const column of allColumns) {
            const dependencyColumns = this.findDependencies(column);
            column.assignDependencies(dependencyColumns);

            if ( column.isRoot() ) {
                this.rootColumns.push( column );
            }
        }

        this.rootColumns = uniqBy(this.rootColumns, (column) => column.getId());
    }

    private findDependencies(column: CacheColumn) {
        const dependencyColumns = column.getColumnRefs().map((depColumnRef) => 
            this.getColumn(depColumnRef.tableReference, depColumnRef.name)!
        ).filter(column => !!column);

        return uniqBy(dependencyColumns, (column) => column.getId());
    }
}

function buildChangesMap(
    columns: CacheColumn[],
    map: Record<string, boolean> = {}
): Record<string, boolean> {
    for (const column of columns) {
        map[ column.getId() ] = true;
        buildChangesMap(column.findNotCircularUses(), map);
    }
    return map;
}