import { AbstractComparator } from "./AbstractComparator";
import { Column } from "../database/schema/Column";
import { Comment } from "../database/schema/Comment";
import {
    Select,
    SelectColumn,
    FuncCall,
    Expression,
    ColumnReference,
    UnknownExpressionElement
} from "../ast";
import { CacheTriggersBuilder, IOutputTrigger } from "../cache/CacheTriggersBuilder";
import { AggFactory } from "../cache/aggregator";
import { flatMap } from "lodash";
import {
    ISortSelectItem,
    sortSelectsByDependencies,
    findRecursionUpdates
} from "./graph-util";
import { TableID } from "../database/schema/TableID";
import { IUpdate, Migration } from "../Migrator/Migration";
import { TableReference } from "../database/schema/TableReference";
import { IDatabaseDriver } from "../database/interface";
import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";

export class CacheComparator extends AbstractComparator {

    private sortedSelectsForEveryColumn: ISortSelectItem[];
    private allCacheTriggers: IOutputTrigger[];

    constructor(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState,
        migration: Migration
    ) {
        super(driver, database, fs, migration);

        this.allCacheTriggers = [];
        const allSelectsForEveryColumn: ISortSelectItem[] = [];

        for (const cache of this.fs.allCache()) {
            const cacheTriggerFactory = new CacheTriggersBuilder(
                cache,
                this.database
            );

            const cacheTriggers = cacheTriggerFactory.createTriggers();
    
            for (const trigger of cacheTriggers) {
                this.allCacheTriggers.push(trigger);
            }

            for (const toUpdate of cacheTriggerFactory.createSelectsForUpdate()) {
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

        this.sortedSelectsForEveryColumn = sortSelectsByDependencies(
            allSelectsForEveryColumn
        );
    }

    async drop() {
        this.dropTrashTriggers();
        this.dropTrashFuncs();
        await this.dropTrashColumns();
    }

    async create() {
        this.createTriggers();
        await this.createAllColumns();
        this.updateAllColumns();
    }

    async createLogFuncs() {
        for (const trigger of this.allCacheTriggers) {
            this.migration.create({
                functions: [trigger.function]
            });
        }
    }

    async createWithoutUpdates() {
        this.createTriggers();
        await this.createAllColumns();
    }

    async refreshCache() {
        await this.createAllColumns();
        this.forceUpdateAllColumns();
    }

    private dropTrashTriggers() {
        for (const dbCacheTrigger of this.database.allCacheTriggers()) {
            const existsSameCacheTrigger = this.allCacheTriggers.some(item => 
                item.trigger.equal( dbCacheTrigger )
            );
            if ( !existsSameCacheTrigger ) {
                this.migration.drop({
                    triggers: [dbCacheTrigger]
                });
            }
        }
    }

    private dropTrashFuncs() {
        const allCacheFuncs = this.database.functions.filter(func =>
            !!func.cacheSignature
        );
        for (const dbCacheFunc of allCacheFuncs) {
            const existsSameCacheFunc = this.allCacheTriggers.some(item =>
                item.function.equal( dbCacheFunc )
            );
            if ( !existsSameCacheFunc ) {
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

    private async existsCacheWithSameColumn(table: TableID, column: Column) {
        for (const toUpdate of this.sortedSelectsForEveryColumn) {
            if ( !toUpdate.for.equal(table) ) {
                continue;
            }
            if ( toUpdate.select.columns[0].name !== column.name ) {
                continue;
            }

            const newColumnType = await this.getColumnType(
                toUpdate.for,
                toUpdate.select
            );

            if ( column.type.equal(newColumnType) ) {
                return true;
            }
        }

        return false;
    }

    private createTriggers() {
        for (const {trigger, function: func} of this.allCacheTriggers) {
            
            const existFunc = this.database.functions.some(existentFunc =>
                existentFunc.equal(func)
            );
            const table = this.database.getTable( trigger.table );
            const existsTrigger = table && table.triggers.some(existentTrigger =>
                existentTrigger.equal( trigger )
            );

            this.migration.create({
                triggers: existFunc && existsTrigger ? [] : [trigger],
                functions: existFunc ? [] : [func]
            });
        }
    }

    private async createAllColumns() {

        for (const toUpdate of this.sortedSelectsForEveryColumn) {
            const selectColumn = toUpdate.select.columns[0] as SelectColumn;
            const columnName = selectColumn.name;

            const columnToCreate = new Column(
                toUpdate.for.table,
                columnName,
                await this.getColumnType(
                    toUpdate.for,
                    toUpdate.select
                ),
                this.getColumnDefault(toUpdate.select),
                Comment.fromFs({
                    objectType: "column",
                    cacheSignature: toUpdate.cache.getSignature(),
                    cacheSelect: toUpdate.select.toString()
                })
            );

            const table = this.database.getTable( toUpdate.for.table );
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

        this.recreateDepsTriggers();
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

    private updateAllColumns() {
        const allUpdates = this.generateAllUpdates();

        const requiredUpdates: IUpdate[] = allUpdates
            .map((update) => {
                const selectWithRequiredColumns = update.select.cloneWith({
                    columns: update.select.columns.filter(selectColumn => {
                        const existentTable = this.database.getTable(update.forTable.table);
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
                    cacheName: update.cacheName,
                    select: selectWithRequiredColumns,
                    forTable: update.forTable
                };

                requiredUpdate.recursionWith = findRecursionUpdates(
                    update,
                    allUpdates
                );
                requiredUpdate.isFirst = true;

                return requiredUpdate;
            })
            .filter(update =>
                update.select.columns.length > 0
            );
        
        this.migration.create({
            updates: requiredUpdates
        });
    }

    private forceUpdateAllColumns() {
        const allUpdates = this.generateAllUpdates();
        this.migration.create({
            updates: allUpdates
        });
    }

    private generateAllUpdates() {
        const updates: IUpdate[] = [];

        for (let i = 0, n = this.sortedSelectsForEveryColumn.length; i < n; i++) {
            const prevItem = this.sortedSelectsForEveryColumn[ i ];

            const selectsToUpdateOneTable: Select[] = [prevItem.select];
            for (let j = i + 1; j < n; j++) {
                const nextItem = this.sortedSelectsForEveryColumn[ j ];
                const isSimilarSelect = (
                    nextItem.for.equal(prevItem.for) &&
                    nextItem.cache.name === prevItem.cache.name
                );
                if ( !isSimilarSelect ) {
                    break;
                }

                i++;
                selectsToUpdateOneTable.push(nextItem.select);
            }

            const columnsToUpdate = selectsToUpdateOneTable.map(selectToUpdateOneTable =>
                selectToUpdateOneTable.columns[0] as SelectColumn
            );

            updates.push({
                cacheName: prevItem.cache.name,
                forTable: prevItem.for,
                select: prevItem.select.cloneWith({
                    columns: columnsToUpdate
                })
            });
        }

        return updates;
    }

    private async getColumnType(forTable: TableReference, select: Select) {

        let {expression} = select.columns[0] as SelectColumn;

        const explicitCastType = expression.getExplicitCastType();
        if ( explicitCastType ) {
            return explicitCastType;
        }

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

        if ( expression.isNotExists() ) {
            return "boolean";
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

            if ( funcCall.name === "bool_or" || funcCall.name === "bool_and" ) {
                return "boolean";
            }

            if ( funcCall.name === "array_agg" ) {
                const firstArg = funcCall.args[0] as Expression;
                const columnRef = firstArg.getColumnReferences()[0];
    
                if ( columnRef && firstArg.elements.length === 1 ) {        
                    const dbColumn = this.findDbColumnByRef(columnRef);
                    
                    if ( dbColumn ) {
                        return dbColumn.type + "[]";
                    }

                    if ( columnRef.name === "id" ) {
                        return "integer[]";
                    }
                }
            }

            const newFunc = this.migration.toCreate.functions.find(func =>
                func.equalName(funcCall.name)
            );
            if ( newFunc && newFunc.returns.type ) {
                return newFunc.returns.type;
            }
        }

        if ( expression.isColumnReference() ) {
            const columnRef = expression.elements[0] as ColumnReference;
            const dbColumn = this.findDbColumnByRef(columnRef);
            if ( dbColumn ) {
                return dbColumn.type.toString();
            }
        }

        if ( expression.isArrayItemOfColumnReference() ) {
            const columnRef = expression.elements[0] as ColumnReference;
            const dbColumn = this.findDbColumnByRef(columnRef);

            if ( dbColumn && dbColumn.type.isArray() ) {
                const arrayType = dbColumn.type.toString();
                const elemType = arrayType.slice(0, -2);
                return elemType;
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

    private findDbColumnByRef(columnRef: ColumnReference) {
        const dbTable = this.database.getTable(
            columnRef.tableReference.table
        );
        const dbColumn = dbTable && dbTable.getColumn(columnRef.name);

        if ( dbColumn ) {
            return dbColumn;
        }

        const maybeIsCreatingNow = this.migration.toCreate.columns.find(newColumn =>
            newColumn.name === columnRef.name &&
            newColumn.table.equal(columnRef.tableReference.table)
        );
        if ( maybeIsCreatingNow ) {
            return maybeIsCreatingNow;
        }
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

                const toUpdateThatColumn = this.sortedSelectsForEveryColumn.find(toUpdate =>
                    toUpdate.for.equal(columnRef.tableReference.table) &&
                    toUpdate.select.columns[0].name === columnRef.name
                );
                if ( toUpdateThatColumn ) {
                    const newColumnType = await this.getColumnType(
                        toUpdateThatColumn.for,
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
                            `(null::${ columnType })`
                        )
                    );

                    return selectColumn.replaceExpression(newExpression);
                })
            });
        }

        return replacedSelect;
    }

    private getColumnDefault(select: Select) {
        const selectColumn = select.columns[0] as SelectColumn;

        const aggFactory = new AggFactory(
            this.database,
            selectColumn
        );
        const aggregations = aggFactory.createAggregations();
        const agg = Object.values(aggregations)[0];

        if ( agg ) {
            const defaultExpression = agg.default();
            return defaultExpression;
        }
        else if ( /not exists/.test(selectColumn.expression.toString()) ) {
            return "false";
        }
        else {
            // TODO: detect coalesce(x, some)
            return "null";
        }
    }
}