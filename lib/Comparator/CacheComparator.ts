import { AbstractComparator } from "./AbstractComparator";
import { Column } from "../database/schema/Column";
import { Comment } from "../database/schema/Comment";
import {
    Cache,
    Select,
    From,
    SelectColumn,
    FuncCall,
    Expression,
    ColumnReference,
    UnknownExpressionElement
} from "../ast";
import { CacheTriggersBuilder } from "../cache/CacheTriggersBuilder";
import { AggFactory } from "../cache/aggregator";
import { flatMap } from "lodash";
import {
    ISortSelectItem,
    sortSelectsByDependencies
} from "./graph-util";
import { TableID } from "../database/schema/TableID";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { IUpdate } from "../Migrator/Migration";

export class CacheComparator extends AbstractComparator {

    async drop() {
        this.dropTrashTriggers();
        this.dropTrashFuncs();
        await this.dropTrashColumns();
    }

    async create() {
        const {sortedSelectsForEveryColumn} = await this.createColumnsAndTriggers();
        this.updateAllColumns(
            sortedSelectsForEveryColumn
        );
    }

    async createLogFuncs() {
        const allCache = flatMap(this.fs.files, file => file.content.cache);

        for (const cache of allCache) {
            const cacheTriggerFactory = new CacheTriggersBuilder(
                cache,
                this.database
            );
            const cacheTriggers = cacheTriggerFactory.createTriggers();
    
            for (const trigger of cacheTriggers) {
                this.migration.create({
                    functions: [trigger.function]
                });
            }
        }
    }

    async createWithoutUpdates() {
        await this.createColumnsAndTriggers();
    }

    async refreshCache() {
        const allCache = flatMap(this.fs.files, file => file.content.cache);
        const sortedSelectsForEveryColumn = this.sortSelectsByDependencies(allCache);

        await this.createAllColumns(sortedSelectsForEveryColumn);
        this.forceUpdateAllColumns(sortedSelectsForEveryColumn);
    }

    private dropTrashTriggers() {
        const allCacheTriggers = flatMap(this.database.tables, 
            table => table.triggers
        ).filter(trigger => !!trigger.cacheSignature);

        for (const dbCacheTrigger of allCacheTriggers) {
            this.dropTriggerIfNotExistsCache(dbCacheTrigger);
        }
    }

    private dropTrashFuncs() {
        const allCacheFuncs = this.database.functions.filter(func =>
            !!func.cacheSignature
        );
        for (const dbCacheFunc of allCacheFuncs) {
            this.dropFuncIfNotExistsCache(dbCacheFunc);
        }

    }

    private async dropTrashColumns() {
        for (const table of this.database.tables) {
            for (const column of table.columns) {
                if ( !column.cacheSignature ) {
                    continue;
                }

                const existsCacheWithSameColumn = await this.existsCacheWithSameColumn(
                    table,
                    column
                );
                if ( !existsCacheWithSameColumn ) {
                    this.migration.drop({
                        columns: [column]
                    });
                }
            }
        }
    }

    private dropTriggerIfNotExistsCache(dbCacheTrigger: DatabaseTrigger) {
        const existsCache = this.fs.files.some(file => 
            file.content.cache.some(cache => {

                const cacheTriggerFactory = new CacheTriggersBuilder(
                    cache,
                    this.database
                );
                const triggers = cacheTriggerFactory.createTriggers();
            
                const existsSameCacheFunc = triggers.some(item => 
                    item.trigger.equal( dbCacheTrigger )
                );
                return existsSameCacheFunc;
            })
        );
        if ( !existsCache ) {
            this.migration.drop({
                triggers: [dbCacheTrigger]
            });
        }
    }

    private dropFuncIfNotExistsCache(dbCacheFunc: DatabaseFunction) {
        const existsCache = this.fs.files.some(file => 
            file.content.cache.some(cache => {

                const cacheTriggerFactory = new CacheTriggersBuilder(
                    cache,
                    this.database
                );
                const triggers = cacheTriggerFactory.createTriggers();
            
                const existsSameCacheFunc = triggers.some(item => 
                    item.function.equal( dbCacheFunc )
                );
                return existsSameCacheFunc;
            })
        );
        if ( !existsCache ) {
            const needDropTrigger = flatMap(this.database.tables, table => table.triggers)
                .find(trigger => trigger.procedure.name === dbCacheFunc.name);
            const alreadyDropped = needDropTrigger && this.migration.toDrop.triggers.some(trigger =>
                trigger.equal(needDropTrigger)
            );

            this.migration.drop({
                triggers: needDropTrigger && !alreadyDropped ? [needDropTrigger] : [],
                functions: [dbCacheFunc]
            });
        }
    }

    private async existsCacheWithSameColumn(table: TableID, column: Column) {
        for (const file of this.fs.files) {
            for (const cache of file.content.cache) {

                if ( !cache.for.table.equal(table) ) {
                    continue;
                }

                const selectToUpdate = this.createSelectForUpdate(cache);
                const sameNameSelectColumn = selectToUpdate.columns.find(selectColumn =>
                    selectColumn.name === column.name
                );
                if ( !sameNameSelectColumn ) {
                    continue;
                }

                const newColumnType = await this.getColumnType(
                    cache,
                    selectToUpdate.cloneWith({
                        columns: [sameNameSelectColumn]
                    })
                );

                if ( column.type.equal(newColumnType) ) {
                    return true;
                }
            }
        }

        return false;
    }

    private async createColumnsAndTriggers() {
        const allCache = flatMap(this.fs.files, file => file.content.cache);

        for (const cache of allCache) {
            const cacheTriggerFactory = new CacheTriggersBuilder(
                cache,
                this.database
            );
            const cacheTriggers = cacheTriggerFactory.createTriggers();
    
            for (const {trigger, function: func} of cacheTriggers) {

                const table = this.database.getTable( trigger.table );
                const existsTrigger = table && table.triggers.some(existentTrigger =>
                    existentTrigger.equal( trigger )
                );
                const existFunc = this.database.functions.some(existentFunc =>
                    existentFunc.equal(func)
                );

                this.migration.create({
                    triggers: existFunc && existsTrigger ? [] : [trigger],
                    functions: existFunc ? [] : [func]
                });
            }
        }

        const sortedSelectsForEveryColumn = this.sortSelectsByDependencies(allCache);
        
        await this.createAllColumns(
            sortedSelectsForEveryColumn
        );

        this.recreateDepsTriggers();

        return {sortedSelectsForEveryColumn};
    }

    // fix: cannot drop column because other objects depend on it
    private recreateDepsTriggers() {
        for (const table of this.database.tables) {
            for (const dbTrigger of table.triggers) {
                if ( dbTrigger.frozen ) {
                    continue;
                }
                const hasDropInMigration = this.migration.toDrop.triggers
                    .some(dropTrigger =>
                        dropTrigger.name === dbTrigger.name &&
                        dropTrigger.table.equal(dbTrigger.table)
                    );
                if ( hasDropInMigration ) {
                    continue;
                }

                const table = dbTrigger.table;
                const hasDepsToCacheColumn = (dbTrigger.updateOf || [])
                    .some(triggerDepsColumnName => {
                        const thisColumnNeedDrop = this.migration.toDrop.columns
                            .some(columnToDrop =>
                                triggerDepsColumnName === columnToDrop.name &&
                                columnToDrop.table.equal(table)
                            );
                        return thisColumnNeedDrop;
                    });

                if ( hasDepsToCacheColumn ) {
                    this.migration.drop({
                        triggers: [dbTrigger]
                    });
                    this.migration.create({
                        triggers: [dbTrigger]
                    });
                }
            }
        }
    }

    private async createAllColumns(sortedSelectsForEveryColumn: ISortSelectItem[]) {

        for (const {cache, select} of sortedSelectsForEveryColumn) {
            const selectColumn = select.columns[0] as SelectColumn;
            const columnName = selectColumn.name;

            const selectToUpdate = this.createSelectForUpdate(cache)
                .cloneWith({
                    columns: [selectColumn]
                });
            
            const columnToCreate = new Column(
                cache.for.table,
                columnName,
                await this.getColumnType(cache, select),
                this.getColumnDefault(select),
                Comment.fromFs({
                    objectType: "column",
                    cacheSignature: cache.getSignature(),
                    cacheSelect: selectToUpdate.toString()
                })
            );

            const table = this.database.getTable( cache.for.table );
            const existentColumn = table && table.getColumn(columnName);
            const existsSameColumn = (
                existentColumn && 
                existentColumn.equal( columnToCreate )
            );

            if ( !existsSameColumn ) {
                this.migration.create({
                    columns: [columnToCreate]
                });
            }
        }
    }

    private updateAllColumns(sortedSelectsForEveryColumn: ISortSelectItem[]) {
        const allUpdates = this.generateAllUpdates(sortedSelectsForEveryColumn);
        const requiredUpdates: IUpdate[] = allUpdates
            .map((update) => {
                const {cache: cacheToCreate, select} = update;

                const selectWithRequiredColumns = select.cloneWith({
                    columns: select.columns.filter(selectColumn => {
                        const existentTable = this.database.getTable(cacheToCreate.for.table);
                        const existentColumn = existentTable && existentTable.getColumn(selectColumn.name);
        
                        if ( !existentColumn ) {
                            return true;
                        }
        
                        const selectToUpdate = this.createSelectForUpdate(cacheToCreate)
                            .cloneWith({
                                columns: [selectColumn]
                            });
                        
                        const changedUpdateExpression = (
                            existentColumn.comment.cacheSelect !== selectToUpdate.toString()
                        );
                        return changedUpdateExpression;
                    })
                });
                
                const requiredUpdate: IUpdate = {
                    cacheName: cacheToCreate.name,
                    select: selectWithRequiredColumns,
                    forTable: cacheToCreate.for
                };
                return requiredUpdate;
            })
            .filter(update =>
                update.select.columns.length > 0
            );
        
        this.migration.create({
            updates: requiredUpdates
        });
    }

    private forceUpdateAllColumns(sortedSelectsForEveryColumn: ISortSelectItem[]) {
        const allUpdates: IUpdate[] = this.generateAllUpdates(sortedSelectsForEveryColumn)
            .map(inputUpdate => {
                const update: IUpdate = {
                    cacheName: inputUpdate.cache.name,
                    select: inputUpdate.select,
                    forTable: inputUpdate.cache.for
                };
                return update;
            });
        
        this.migration.create({
            updates: allUpdates
        });
    }

    private generateAllUpdates(sortedSelectsForEveryColumn: ISortSelectItem[]) {
        const updates: ISortSelectItem[] = [];

        for (let i = 0, n = sortedSelectsForEveryColumn.length; i < n; i++) {
            const {select, cache: cacheToCreate} = sortedSelectsForEveryColumn[ i ];

            const selectsToUpdateOneTable: Select[] = [select];
            for (let j = i + 1; j < n; j++) {
                const nextItem = sortedSelectsForEveryColumn[ j ];
                if ( nextItem.cache !== cacheToCreate ) {
                    break;
                }

                i++;
                selectsToUpdateOneTable.push(nextItem.select);
            }

            const columnsToOnlyRequiredUpdate = selectsToUpdateOneTable.map(selectToUpdateOneTable =>
                selectToUpdateOneTable.columns[0] as SelectColumn
            );

            const selectToUpdate = this.createSelectForUpdate(cacheToCreate)
                .cloneWith({
                    columns: columnsToOnlyRequiredUpdate
                });
            
            updates.push({
                select: selectToUpdate,
                cache: cacheToCreate
            });
        }

        return updates;
    }

    private async getColumnType(cache: Cache, select: Select) {

        let {expression} = select.columns[0] as SelectColumn;

        if ( expression.isFuncCall() ) {
            const funcCall = expression.getFuncCalls()[0] as FuncCall;
            if ( funcCall.name === "coalesce" ) {
                expression = funcCall.args[0] as Expression;
            }
        }

        if ( expression.isFuncCall() ) {
            const funcCall = expression.getFuncCalls()[0] as FuncCall;
            if ( funcCall.name === "max" || funcCall.name === "min" ) {
                expression = funcCall.args[0] as Expression;
            }
        }

        if ( expression.isFuncCall() ) {
            const funcCall = expression.getFuncCalls()[0] as FuncCall;

            if ( funcCall.name === "count" ) {
                return "bigint";
            }

            if ( funcCall.name === "string_agg" ) {
                return "text";
            }

            if ( funcCall.name === "sum" ) {
                return "numeric";
            }

            if ( funcCall.name === "array_agg" ) {
                const firstArg = funcCall.args[0] as Expression;
                const columnRef = firstArg.getColumnReferences()[0];
    
                if ( columnRef && firstArg.elements.length === 1 ) {        
                    const table = this.database.getTable(
                        columnRef.tableReference.table
                    );
                    const column = table && table.getColumn(columnRef.name);
                    
                    if ( column ) {
                        return column.type + "[]";
                    }

                    if ( columnRef.name === "id" ) {
                        return "integer[]";
                    }
                }
            }

            const newFunc = this.migration.toCreate.functions.find(func =>
                func.name === funcCall.name
            );
            if ( newFunc && newFunc.returns.type ) {
                return newFunc.returns.type;
            }
        }

        if ( expression.isColumnReference() ) {
            const columnRef = expression.elements[0] as ColumnReference;
            const dbTable = this.database.getTable(
                columnRef.tableReference.table
            );
            const dbColumn = dbTable && dbTable.getColumn(columnRef.name);

            if ( dbColumn ) {
                return dbColumn.type.toString();
            }
        }

        const selectWithReplacedColumns = await this.replaceUnknownColumns(select);
        const columnsTypes = await this.driver.getCacheColumnsTypes(
            selectWithReplacedColumns.cloneWith({
                columns: [
                    selectWithReplacedColumns.columns[0]
                ]
            }),
            cache.for
        );

        const columnType = Object.values(columnsTypes)[0];
        return columnType;
    }

    private async replaceUnknownColumns(select: Select) {
        let replacedSelect = select;

        for (const selectColumn of select.columns) {
            const columnRefs = selectColumn.expression.getColumnReferences();
            for (const columnRef of columnRefs) {
                const dbTable = this.database.getTable(
                    columnRef.tableReference.table
                );
                const dbColumn = dbTable && dbTable.getColumn(columnRef.name);
                if ( dbColumn ) {
                    continue;
                }

                const maybeIsCreatingNow = this.migration.toCreate.columns.find(newColumn =>
                    newColumn.name === columnRef.name &&
                    newColumn.table.equal(columnRef.tableReference.table)
                );
                if ( maybeIsCreatingNow ) {
                    replace(
                        columnRef,
                        maybeIsCreatingNow.type.toString()
                    );
                    continue;
                }

                const maybeExistsCache = flatMap(this.fs.files, (file) => 
                    file.content.cache
                ).find(cache =>
                    cache.for.table.equal(columnRef.tableReference.table) &&
                    cache.select.columns.some(column =>
                        column.name === columnRef.name
                    )
                );
                if ( maybeExistsCache ) {

                    const selectToUpdate = this.createSelectForUpdate(maybeExistsCache);
                    const sameNameSelectColumn = selectToUpdate.columns.find(selectColumn =>
                        selectColumn.name === columnRef.name
                    )!;

                    const newColumnType = await this.getColumnType(
                        maybeExistsCache,
                        selectToUpdate.cloneWith({
                            columns: [sameNameSelectColumn]
                        })
                    );

                    replace(
                        columnRef,
                        newColumnType
                    );
                }
            }
        }

        function replace(
            columnRef: ColumnReference,
            columnType: string
        ) {
            replacedSelect = replacedSelect.cloneWith({
                columns: replacedSelect.columns.map(selectColumn => {
                    const newExpression = selectColumn.expression.replaceColumn(
                        columnRef,
                        UnknownExpressionElement.fromSql(
                            `null::${ columnType }`
                        )
                    );

                    return selectColumn.replaceExpression(newExpression);
                })
            });
        }

        return replacedSelect;
    }

    private getColumnDefault(select: Select) {
        const aggFactory = new AggFactory(
            this.database,
            select.columns[0] as SelectColumn
        );
        const aggregations = aggFactory.createAggregations();
        const agg = Object.values(aggregations)[0];

        if ( agg ) {
            const defaultExpression = agg.default();
            return defaultExpression;
        }
        else {
            // TODO: detect coalesce(x, some)
            return "null";
        }
    }


    private sortSelectsByDependencies(allCache: Cache[]) {
        // one cache can dependent on other cache
        // need build all columns before package updates
        const allSelectsForEveryColumn = this.generateAllSelectsForEveryColumn(allCache);
        const sortedSelectsForEveryColumn = sortSelectsByDependencies(
            allSelectsForEveryColumn
        );

        return sortedSelectsForEveryColumn;
    }

    private generateAllSelectsForEveryColumn(allCache: Cache[]) {

        const allSelectsForEveryColumn = flatMap(allCache, cache => {

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
            this.database
        );
        const selectToUpdate = cacheTriggerFactory.createSelectForUpdate();
        return selectToUpdate;
    }
}