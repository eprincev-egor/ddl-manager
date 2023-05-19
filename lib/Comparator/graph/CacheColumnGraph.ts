import { CacheColumn, CacheColumnParams } from "./CacheColumn";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { ColumnReference } from "../../ast";
import { flatMap, uniqBy } from "lodash";
import { CacheUpdate } from "./CacheUpdate";
import { buildDependencyMatrix, groupByTables } from "./utils";

export class CacheColumnGraph {

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

    getDependencyIndex(column: CacheColumn) {
        return flatMap(this.dependencyMatrix).findIndex(levelColumn =>
            levelColumn.name === column.name &&
            levelColumn.for.table.equal(column.for.table)
        );
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
        const dependencyColumns = flatMap(column.getColumnRefs(), (depColumnRef) => 
            this.findDependenciesByRef(depColumnRef)
        );
        return uniqBy(dependencyColumns, (column) => column.getId());
    }

    private findDependenciesByRef(columnRef: ColumnReference) {
        return this.getColumns(columnRef.tableReference)
            .filter(column => column.name == columnRef.name);
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