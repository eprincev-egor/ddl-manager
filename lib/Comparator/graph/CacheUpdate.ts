import { CacheColumn } from "./CacheColumn";
import { Select } from "../../ast";
import { TableReference } from "../../database/schema/TableReference";
import { flatMap, groupBy, uniq } from "lodash";
import { groupByTables } from "./utils";

export class CacheUpdate {

    static fromManyTables(allColumns: CacheColumn[]): CacheUpdate[] {
        const byTable = groupByTables(allColumns);
        const parallelUpdates = Object.values(byTable)
            .map(tableColumns => new CacheUpdate(tableColumns));

        return parallelUpdates;
    }

    table: TableReference;
    selects: Select[];
    columns: CacheColumn[];
    caches: string[];
    recursionWith: CacheUpdate[];
    recursionParent?: CacheUpdate;

    constructor(
        columns: CacheColumn[],
        recursionParent?: CacheUpdate
    ) {
        const table = columns[0].for.table;

        this.table = new TableReference(table, "__updating_table");
        this.columns = columns;
        this.caches = uniq(columns.map(column => column.cache.name));
        this.selects = this.combineSameCacheSelects();
        this.recursionWith = [];
        if ( !recursionParent ) {
            this.recursionWith = this.buildRecursionUpdates();
        }
    }

    private combineSameCacheSelects() {
        const selects: Select[] = [];

        for (const column of this.columns) {
            const newSelect = column.select.replaceTable(column.for, this.table);
            const sameSelectIndex = selects.findIndex(someSelect =>
                someSelect.equalSource(newSelect)
            );
            if ( sameSelectIndex != -1 ) {
                const sameSelect = selects[sameSelectIndex];
                
                const newColumn = column.select.columns[0]
                    .replaceTable(column.for, this.table);

                selects[sameSelectIndex] = sameSelect.addColumn(newColumn);
            }
            else {
                selects.push(newSelect);
            }
        }

        return selects;
    }

    private buildRecursionUpdates() {
        const recursionColumns = flatMap(this.columns,  column => 
            column.findCircularUses()
        );
        const recursionColumnsMatrix = groupBy(recursionColumns, column =>
            column.getTableId()
        );
        const recursionUpdates = Object.values(recursionColumnsMatrix)
            .map(columns => new CacheUpdate(columns, this));
        return recursionUpdates;
    }
}
