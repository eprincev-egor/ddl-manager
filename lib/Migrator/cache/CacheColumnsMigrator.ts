import { AbstractMigrator } from "../AbstractMigrator";
import assert from "assert";
import { CacheTriggersBuilder } from "../../cache/CacheTriggersBuilder";
import { Cache, From, Select, SelectColumn, TableReference } from "../../ast";
import { AbstractAgg, AggFactory } from "../../cache/aggregator";
import { Database as DatabaseStructure } from "../../cache/schema/Database";

interface ISortSelectItem {
    select: Select;
    cache: Cache;
}

export class CacheColumnsMigrator extends AbstractMigrator {
    async drop() {
        for (const cache of this.diff.drop.cache) {
            await this.dropCacheColumns(cache);
        }
    }

    async create() {
        // one cache can dependent on other cache
        // need build all columns before package updates
        await this.createAndUpdateAllCacheColumns();
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


    private async createAndUpdateAllCacheColumns() {
        
        const allSelectsForEveryColumn = this.generateAllSelectsForEveryColumn();
        const sortedSelectsForEveryColumn = this.sortSelectsByDependencies(
            allSelectsForEveryColumn
        );
        
        await this.createAllColumns(
            sortedSelectsForEveryColumn
        );
        await this.updateAllColumns(
            sortedSelectsForEveryColumn
        );
    }

    private async createAllColumns(sortedSelectsForEveryColumn: ISortSelectItem[]) {

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

    private sortSelectsByDependencies(allSelectsForEveryColumn: ISortSelectItem[]) {

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

        return sortedSelectsForEveryColumn;
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