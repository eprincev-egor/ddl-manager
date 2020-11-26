import assert from "assert";
import { CacheTriggersBuilder } from "../cache/CacheTriggersBuilder";
import { Cache, From, Select, SelectColumn, TableReference } from "../ast";
import { AbstractAgg, AggFactory } from "../cache/aggregator";
import { Database as DatabaseStructure } from "../cache/schema/Database";
import { AbstractMigrator } from "./AbstractMigrator";

interface ISortSelectItem {
    select: Select;
    cache: Cache;
}

export class CacheMigrator extends AbstractMigrator {

    async drop() {
        await this.dropCache();
    }

    async create() {
        await this.createAllCache();
    }

    private async dropCache() {
        for (const cache of this.diff.drop.cache) {

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

            const triggersByTableName = cacheTriggerFactory.createTriggers();
            for (const {trigger, function: func} of Object.values(triggersByTableName)) {

                try {
                    await this.postgres.forceDropTrigger(trigger);
                } catch(err) {
                    this.onError(cache, err);
                }
                
                try {
                    await this.postgres.forceDropFunction(func);
                } catch(err) {
                    this.onError(cache, err);
                }
            }
        }
    }


    private async createAllCache() {
        // one cache can dependent on other cache
        // need build all columns before package updates
        await this.createAndUpdateAllCacheColumns();

        for (const cache of this.diff.create.cache || []) {
            await this.createCacheTriggers(cache);
        }

        await this.postgres.saveCacheMeta(this.diff.create.cache);
    }

    private async createAndUpdateAllCacheColumns() {
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

        // sort selects be dependencies
        const sortedSelectsForEveryColumn = allSelectsForEveryColumn
            .filter(item =>
                isRoot(allSelectsForEveryColumn, item)
            );

        for (const prevItem of sortedSelectsForEveryColumn) {
    
            // ищем те, которые явно указали, что они будут после prevItem
            const nextItems = allSelectsForEveryColumn.filter((nextItem) =>
                dependentOn(nextItem, prevItem)
            );
    
            for (let j = 0, m = nextItems.length; j < m; j++) {
                const nextItem = nextItems[ j ];
    
                // если в очереди уже есть этот элемент
                const index = sortedSelectsForEveryColumn.indexOf(nextItem);
                //  удалим дубликат
                if ( index !== -1 ) {
                    sortedSelectsForEveryColumn.splice(index, 1);
                }
    
                //  и перенесем в конец очереди,
                //  таким образом, если у элемента есть несколько "after"
                //  то он будет постоянно уходить в конец после всех своих "after"
                sortedSelectsForEveryColumn.push(nextItem);
            }
        }
        
        
        for (const {select, cache} of sortedSelectsForEveryColumn) {
            const columnsTypes = await this.postgres.getCacheColumnsTypes(
                select,
                cache.for
            );

            const [columnName, columnType] = Object.entries(columnsTypes)[0];

            const aggFactory = new AggFactory(
                select,
                select.columns[0] as SelectColumn
            );
            const aggregations = aggFactory.createAggregations();
            const agg = Object.values(aggregations)[0] as AbstractAgg;

            await this.postgres.createOrReplaceColumn(
                cache.for.table,
                {
                    key: columnName,
                    type: columnType,
                    // TODO: detect default by expression
                    default: agg.default()
                }
            );
        }

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

    private async createCacheTriggers(cache: Cache) {
        const cacheTriggerFactory = new CacheTriggersBuilder(
            cache,
            new DatabaseStructure([])
        );
        const triggersByTableName = cacheTriggerFactory.createTriggers();

        for (const tableName in triggersByTableName) {
            const {trigger, function: func} = triggersByTableName[ tableName ];

            try {
                await this.postgres.createOrReplaceCacheTrigger(trigger, func);
            } catch(err) {
                this.onError(cache, err);
            }
        }
    }

}


function isRoot(allItems: ISortSelectItem[], item: ISortSelectItem) {
    const hasDependencies = allItems.some(prevItem =>
        prevItem !== item &&
        dependentOn(item, prevItem)
    );
    return !hasDependencies;
}

// x dependent on y ?
function dependentOn(
    xItem: ISortSelectItem,
    yItem: ISortSelectItem
): boolean {
    
    const xColumn = xItem.select.columns[0];
    const yColumn = yItem.select.columns[0];

    assert.ok(xColumn);
    assert.ok(yColumn);

    const xRefs = xColumn.expression.getColumnReferences();
    const xDependentOnY = xRefs.some(xColumnRef =>
        xColumnRef.tableReference.table.equal( yItem.cache.for.table ) &&
        xColumnRef.name === yColumn.name
    );

    return xDependentOnY;
}