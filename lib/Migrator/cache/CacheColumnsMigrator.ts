import { AbstractMigrator } from "../AbstractMigrator";
import { CacheTriggersBuilder } from "../../cache/CacheTriggersBuilder";
import { Cache, From, Select, SelectColumn, TableReference } from "../../ast";
import { AbstractAgg, AggFactory } from "../../cache/aggregator";
import { Database as DatabaseStructure } from "../../cache/schema/Database";
import {
    ISortSelectItem,
    sortSelectsByDependencies
} from "./graph-util";
import { flatMap } from "lodash";

export class CacheColumnsMigrator extends AbstractMigrator {
    private databaseStructure: DatabaseStructure = new DatabaseStructure([]);

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
        const allCacheColumns = flatMap(this.diff.drop.cache, cache => {
            const selectToUpdate = this.createSelectForUpdate(cache);
        
            const columns = selectToUpdate.columns
                .map(updateColumn => ({
                    columnName: updateColumn.name,
                    cache
                }));
            
            return columns;
        });
        const trashColumns = allCacheColumns.filter(({columnName: columnNameToDrop, cache: cacheToDrop}) => {
            const cachesOnThatTableForCreate = this.diff.create.cache.filter(cacheToCreate =>
                cacheToCreate.for.table.equal(cacheToDrop.for.table)
            );

            const existsSameColumnToCreate = cachesOnThatTableForCreate.some(cacheToCreate => {
                const selectToUpdate = this.createSelectForUpdate(cacheToCreate);
                const existsSameColumn = selectToUpdate.columns.some(column => 
                    column.name === columnNameToDrop
                );
                return existsSameColumn;
            });

            // TODO: also check type
            return !existsSameColumnToCreate;
        });

        for (const {columnName, cache} of trashColumns) {
            const table = cache.for.table;
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
        const columnType = Object.values(columnsTypes)[0];
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
            const {select, cache: cacheToCreate} = sortedSelectsForEveryColumn[ i ];
            const columnsToUpdate: SelectColumn[] = [select.columns[0] as SelectColumn];

            for (let j = i + 1; j < n; j++) {
                const nextItem = sortedSelectsForEveryColumn[ j ];
                if ( nextItem.cache !== cacheToCreate ) {
                    break;
                }

                columnsToUpdate.push(nextItem.select.columns[0] as SelectColumn);
            }

            const columnsToOnlyRequiredUpdate = columnsToUpdate.filter(columnToCreate => {
                const cachesToDropOnThatTable = this.diff.drop.cache.filter(cacheToDrop =>
                    cacheToDrop.for.table.equal( cacheToCreate.for.table )
                );

                const existsSameColumnToDrop = cachesToDropOnThatTable.some(cacheToDrop => {
                    const selectToUpdateByDropCache = this.createSelectForUpdate(cacheToCreate);
                    const existsSameColumn = selectToUpdateByDropCache.columns.some(columnNameToDrop => 
                        columnToCreate.name === columnNameToDrop.name
                    );
                    return existsSameColumn;
                });

                return !existsSameColumnToDrop;
            });

            if ( !columnsToOnlyRequiredUpdate.length ) {
                continue;
            }

            const selectToUpdate = this.createSelectForUpdate(cacheToCreate)
                .cloneWith({
                    columns: columnsToOnlyRequiredUpdate
                });
            
            await this.updateCachePackage(
                selectToUpdate,
                cacheToCreate.for
            );
        }
    }

    private generateAllSelectsForEveryColumn() {

        const allSelectsForEveryColumn = flatMap(this.diff.create.cache, cache => {

            const selectToUpdate = this.createSelectForUpdate(cache);
            
            return selectToUpdate.columns.map(updateColumn => {
                const selectThatColumn = new Select({
                    columns: [updateColumn],
                    from: selectToUpdate.getAllTableReferences().map(tableRef =>
                        new From(tableRef)
                    )
                });

                return {
                    select: selectThatColumn,
                    cache
                };
            });
        });

        return allSelectsForEveryColumn;
    }

    private createSelectForUpdate(cache: Cache) {
        const cacheTriggerFactory = new CacheTriggersBuilder(
            cache,
            this.databaseStructure
        );
        const selectToUpdate = cacheTriggerFactory.createSelectForUpdate();
        return selectToUpdate;
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
