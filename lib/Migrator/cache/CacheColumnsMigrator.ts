import { AbstractMigrator } from "../AbstractMigrator";
import { CacheTriggersBuilder } from "../../cache/CacheTriggersBuilder";
import { Cache, From, Select, SelectColumn, TableReference } from "../../ast";
import { AbstractAgg, AggFactory } from "../../cache/aggregator";
import { Database as DatabaseStructure } from "../../cache/schema/Database";
import {
    ISortSelectItem,
    sortSelectsByDependencies
} from "./graph-util";

export class CacheColumnsMigrator extends AbstractMigrator {
    async drop() {
        await this.dropOnlyTrashColumns();
    }

    async create() {
        const sortedSelectsForEveryColumn = this.sortSelectsByDependencies();
        
        await this.createAllColumns(
            sortedSelectsForEveryColumn
        );
        await this.updateAllColumns(
            sortedSelectsForEveryColumn
        );
    }

    async dropOnlyTrashColumns() {
        for (const cache of this.diff.drop.cache) {
            await this.dropCacheColumns(cache);
        }
    }

    async dropCacheColumns(cache: Cache) {
        const cacheTriggerFactory = new CacheTriggersBuilder(
            cache,
            new DatabaseStructure([])
        );
        const selectToUpdate = cacheTriggerFactory.createSelectForUpdate();
        const columns = selectToUpdate.columns.map(col => col.name);
        
        const table = cache.for.table;
        for (const columnName of columns) {
            try {
                await this.postgres.dropColumn(table, columnName);
            } catch(err) {
                this.onError(cache, err);
            }
        }
    }

    private sortSelectsByDependencies() {
        // one cache can dependent on other cache
        // need build all columns before package updates
        const allSelectsForEveryColumn = this.generateAllSelectsForEveryColumn();
        const sortedSelectsForEveryColumn = sortSelectsByDependencies(
            allSelectsForEveryColumn
        );

        return sortedSelectsForEveryColumn;
    }

    private async createAllColumns(sortedSelectsForEveryColumn: ISortSelectItem[]) {

        for (const {cache, select} of sortedSelectsForEveryColumn) {
            await this.createColumn(cache, select);
        }
    }

    private async createColumn(cache: Cache, select: Select) {
        const column = {
            key: (select.columns[0] as SelectColumn).name,
            type: await this.getColumnType(cache, select),
            default: this.getColumnDefault(select)
        };
        await this.postgres.createOrReplaceColumn(
            cache.for.table,
            column
        );
    }

    private async getColumnType(cache: Cache, select: Select) {
        const columnsTypes = await this.postgres.getCacheColumnsTypes(
            select,
            cache.for
        );

        // TODO: detect default by expression
        const [columnName, columnType] = Object.entries(columnsTypes)[0];
        return columnType;
    }

    private getColumnDefault(select: Select) {
        const aggFactory = new AggFactory(
            select,
            select.columns[0] as SelectColumn
        );
        const aggregations = aggFactory.createAggregations();
        const agg = Object.values(aggregations)[0] as AbstractAgg;

        const defaultExpression = agg.default();
        return defaultExpression;
    }

    private async updateAllColumns(sortedSelectsForEveryColumn: ISortSelectItem[]) {

        for (let i = 0, n = sortedSelectsForEveryColumn.length; i < n; i++) {
            const {select, cache} = sortedSelectsForEveryColumn[ i ];
            const columnsToUpdate: SelectColumn[] = [select.columns[0] as SelectColumn];

            for (let j = i + 1; j < n; j++) {
                const nextItem = sortedSelectsForEveryColumn[ j ];
                if ( nextItem.cache !== cache ) {
                    break;
                }

                columnsToUpdate.push(nextItem.select.columns[0] as SelectColumn);
            }

            const cacheTriggerFactory = new CacheTriggersBuilder(
                cache,
                new DatabaseStructure([])
            );
            const selectToUpdate = cacheTriggerFactory.createSelectForUpdate()
                .cloneWith({
                    columns: columnsToUpdate
                });
            
            await this.updateCachePackage(
                selectToUpdate,
                cache.for
            );
        }
    }

    private generateAllSelectsForEveryColumn() {
        const allCaches = this.diff.create.cache || [];

        const allSelectsForEveryColumn: ISortSelectItem[] = [];

        for (const cache of allCaches) {
            const cacheTriggerFactory = new CacheTriggersBuilder(
                cache,
                new DatabaseStructure([])
            );
            const selectToUpdate = cacheTriggerFactory.createSelectForUpdate();
            
            for (const updateColumn of selectToUpdate.columns) {

                const selectThatColumn = new Select({
                    columns: [updateColumn],
                    from: selectToUpdate.getAllTableReferences().map(tableRef =>
                        new From(tableRef)
                    )
                });
                allSelectsForEveryColumn.push({
                    select: selectThatColumn,
                    cache
                });
            }
        }

        return allSelectsForEveryColumn;
    }

    private async updateCachePackage(selectToUpdate: Select, forTableRef: TableReference) {
        const limit = 500;
        let updatedCount = 0;

        do {
            updatedCount = await this.postgres.updateCachePackage(
                selectToUpdate,
                forTableRef,
                limit
            );
        } while( updatedCount >= limit );
    }
}
