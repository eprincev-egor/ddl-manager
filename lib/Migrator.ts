import assert from "assert";
import { CacheTriggerFactory } from "./cache/CacheTriggerFactory";
import { Cache, Expression, From, Select, SelectColumn, TableReference } from "./ast";
import { AbstractAgg, AggFactory } from "./cache/aggregator";
import { Diff } from "./Diff";
import { IDatabaseDriver } from "./database/interface";

interface ISortSelectItem {
    select: Select;
    cache: Cache;
}

export class Migrator {
    private postgres: IDatabaseDriver;
    private cacheTriggerFactory: CacheTriggerFactory;
    private outputErrors: Error[];
    private diff: Diff;

    static async migrate(postgres: IDatabaseDriver, diff: Diff) {
        assert.ok(diff);
        const migrator = new Migrator(postgres, diff);
        return await migrator.migrate();
    }
    
    private constructor(postgres: IDatabaseDriver, diff: Diff) {
        this.postgres = postgres;
        this.diff = diff;
        this.cacheTriggerFactory = new CacheTriggerFactory();
        this.outputErrors = [];
    }

    async migrate() {
        await this.dropTriggers();
        await this.dropFunctions();

        await this.createFunctions();
        await this.createTriggers();
        await this.createAllCache();

        return this.outputErrors;
    }

    private async dropTriggers() {

        for (const trigger of this.diff.drop.triggers) {
            try {
                await this.postgres.dropTrigger(trigger);
            } catch(err) {
                this.onError(trigger, err);
            }
        }
    }

    private async dropFunctions() {

        for (const func of this.diff.drop.functions) {
            // 2BP01
            try {
                await this.postgres.dropFunction(func);
            } catch(err) {
                // https://www.postgresql.org/docs/12/errcodes-appendix.html
                // cannot drop function my_func() because other objects depend on it
                const isCascadeError = err.code === "2BP01";
                if ( !isCascadeError ) {
                    this.onError(func, err);
                }
            }
        }
    }

    private async createFunctions() {

        for (const func of this.diff.create.functions) {
            try {
                await this.postgres.createOrReplaceFunction(func);
            } catch(err) {
                this.onError(func, err);
            }
        }
    }

    private async createTriggers() {

        for (const trigger of this.diff.create.triggers) {
            try {
                await this.postgres.createOrReplaceTrigger(trigger);
            } catch(err) {
                this.onError(trigger, err);
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
    }

    private async createAndUpdateAllCacheColumns() {
        const allCaches = this.diff.create.cache || [];

        const allSelectsForEveryColumn: ISortSelectItem[] = [];

        for (const cache of allCaches) {
            const selectToUpdate = this.cacheTriggerFactory.createSelectForUpdate(cache);
            
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

            const selectToUpdate = this.cacheTriggerFactory.createSelectForUpdate(cache)
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
        const triggersByTableName = this.cacheTriggerFactory.createTriggers(cache);

        for (const tableName in triggersByTableName) {
            const {trigger, function: func} = triggersByTableName[ tableName ];

            try {
                await this.postgres.createOrReplaceFunction(func);
                await this.postgres.createOrReplaceTrigger(trigger);
            } catch(err) {
                this.onError(cache, err);
            }
        }
    }

    private onError(
        obj: {getSignature(): string},
        err: Error
    ) {
        // redefine callstack
        const newErr = new Error(obj.getSignature() + "\n" + err.message);
        (newErr as any).originalError = err;
        
        this.outputErrors.push(newErr);
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