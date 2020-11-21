import assert from "assert";
import { CacheTriggersBuilder } from "./cache/CacheTriggersBuilder";
import { Cache, DatabaseFunction, From, Select, SelectColumn, TableReference } from "./ast";
import { AbstractAgg, AggFactory } from "./cache/aggregator";
import { Diff } from "./Diff";
import { IDatabaseDriver } from "./database/interface";
import { Database as DatabaseStructure } from "./cache/schema/Database";

interface ISortSelectItem {
    select: Select;
    cache: Cache;
}

export class Migrator {
    private postgres: IDatabaseDriver;
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
        this.outputErrors = [];
    }

    async migrate() {
        await this.createCacheHelpersFunctions();

        await this.dropTriggers();
        await this.dropFunctions();

        await this.createFunctions();
        await this.createTriggers();
        await this.createAllCache();

        return this.outputErrors;
    }

    private async createCacheHelpersFunctions() {
        // TODO: parse from files
        
        const CM_ARRAY_REMOVE_ONE_ELEMENT = new DatabaseFunction({
            schema: "public",
            name: "cm_array_remove_one_element",
            args: [
                {name: "input_arr", type: "anyarray"},
                {name: "element_to_remove", type: "anyelement"}
            ],
            returns: {
                type: "anyarray"
            },
            body: `
declare element_position integer;
begin

    element_position = array_position(
        input_arr,
        element_to_remove
    );

    return (
        input_arr[:(element_position - 1)] || 
        input_arr[(element_position + 1):]
    );
    
end
            `
        });
        const CM_ARRAY_TO_STRING_DISTINCT = new DatabaseFunction({
            schema: "public",
            name: "cm_array_to_string_distinct",
            args: [
                {name: "input_arr", type: "text[]"},
                {name: "separator", type: "text"}
            ],
            returns: {
                type: "text"
            },
            body: `
begin
    
    return (
        select 
            string_agg(distinct input_value, separator)
        from unnest( input_arr ) as input_value
    );
end
            `
        });
        const CM_DISTINCT_ARRAY = new DatabaseFunction({
            schema: "public",
            name: "cm_distinct_array",
            args: [
                {name: "input_arr", type: "anyarray"}
            ],
            returns: {
                type: "anyarray"
            },
            body: `
begin
    return (
        select 
            array_agg(distinct input_value)
        from unnest( input_arr ) as input_value
    );
    
end
            `
        });
        // TODO: cm_is_distinct_arrays
        // TODO: cm_get_deleted_elements
        // TODO: cm_get_inserted_elements

        await this.postgres.createOrReplaceHelperFunc(CM_ARRAY_REMOVE_ONE_ELEMENT);
        await this.postgres.createOrReplaceHelperFunc(CM_ARRAY_TO_STRING_DISTINCT);
        await this.postgres.createOrReplaceHelperFunc(CM_DISTINCT_ARRAY);
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