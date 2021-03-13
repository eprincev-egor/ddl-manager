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
import { TableReference } from "../database/schema/TableReference";

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
        const sortedSelectsForEveryColumn = this.sortSelectsByDependencies();

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
        const allSelectsForEveryColumn = this.generateAllSelectsForEveryColumn();
        for (const toUpdate of allSelectsForEveryColumn) {
            if ( !toUpdate.for.equal(table) ) {
                continue;
            }
            if ( toUpdate.select.columns[0].name !== column.name ) {
                continue;
            }

            const newColumnType = await this.getColumnType(
                new TableReference(toUpdate.for),
                toUpdate.select
            );

            if ( column.type.equal(newColumnType) ) {
                return true;
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

        const sortedSelectsForEveryColumn = this.sortSelectsByDependencies();
        
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

        for (const toUpdate of sortedSelectsForEveryColumn) {
            const selectColumn = toUpdate.select.columns[0] as SelectColumn;
            const columnName = selectColumn.name;

            const columnToCreate = new Column(
                toUpdate.for,
                columnName,
                await this.getColumnType(
                    new TableReference(toUpdate.for),
                    toUpdate.select
                ),
                this.getColumnDefault(toUpdate.select),
                Comment.fromFs({
                    objectType: "column",
                    cacheSignature: toUpdate.cache.getSignature(),
                    cacheSelect: toUpdate.select.toString()
                })
            );

            const table = this.database.getTable( toUpdate.for );
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
        
                        const selectToUpdateColumn = update.select.cloneWith({
                            columns: [selectColumn]
                        });
                        
                        const changedUpdateExpression = (
                            existentColumn.comment.cacheSelect !== selectToUpdateColumn.toString()
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

            for (const toUpdate of this.createSelectForUpdate(cacheToCreate)) {
                updates.push({
                    cache: cacheToCreate,
                    for: toUpdate.for,
                    select: toUpdate.select.cloneWith({
                        columns: columnsToOnlyRequiredUpdate
                    })
                });
            }
        }

        return updates;
    }

    private async getColumnType(forTable: TableReference, select: Select) {

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
            new Select({
                columns: [
                    selectWithReplacedColumns.columns[0]
                ],
                from: selectWithReplacedColumns.from
            }),
            forTable
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

                const allSelectsForEveryColumn = this.generateAllSelectsForEveryColumn();
                const toUpdateThatColumn = allSelectsForEveryColumn.find(toUpdate =>
                    toUpdate.for.equal(columnRef.tableReference.table) &&
                    toUpdate.select.columns[0].name === columnRef.name
                );
                if ( toUpdateThatColumn ) {
                    const newColumnType = await this.getColumnType(
                        new TableReference(toUpdateThatColumn.for),
                        toUpdateThatColumn.select
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


    private sortSelectsByDependencies() {
        // one cache can dependent on other cache
        // need build all columns before package updates
        const allSelectsForEveryColumn = this.generateAllSelectsForEveryColumn();
        return sortSelectsByDependencies(
            allSelectsForEveryColumn
        );
    }

    private generateAllSelectsForEveryColumn() {
        const allCache = flatMap(this.fs.files, file => file.content.cache);

        const allSelectsForEveryColumn: ISortSelectItem[] = [];

        for (const cache of allCache) {
            for (const toUpdate of this.createSelectForUpdate(cache)) {
                for (const updateColumn of toUpdate.select.columns) {
                    allSelectsForEveryColumn.push({
                        cache,
                        for: toUpdate.for,
                        select: toUpdate.select.cloneWith({
                            columns: [
                                updateColumn
                            ]
                        })
                    });
                }
            }
        }

        return allSelectsForEveryColumn;
    }

    private createSelectForUpdate(cache: Cache) {
        const cacheTriggerFactory = new CacheTriggersBuilder(
            cache,
            this.database
        );
        const selectToUpdate = cacheTriggerFactory.createSelectsForUpdate();
        return selectToUpdate;
    }
}