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
    ColumnReference 
} from "../ast";
import { CacheTriggersBuilder } from "../cache/CacheTriggersBuilder";
import { AbstractAgg, AggFactory } from "../cache/aggregator";
import { flatMap } from "lodash";
import {
    ISortSelectItem,
    sortSelectsByDependencies
} from "./graph-util";

export class CacheComparator extends AbstractComparator {

    drop() {

        const allCacheFuncs = this.database.functions.filter(func =>
            !!func.cacheSignature
        );
        const allCacheTriggers = flatMap(this.database.tables, 
            table => table.triggers
        ).filter(trigger => !!trigger.cacheSignature);

        for (const dbCacheTrigger of allCacheTriggers) {
            const existsCache = this.fs.files.some(file => 
                file.content.cache.some(cache => {

                    const cacheTriggerFactory = new CacheTriggersBuilder(
                        cache,
                        this.database
                    );
                    const triggersByTableName = cacheTriggerFactory.createTriggers();
                
                    const existsSameCacheFunc = Object.values(triggersByTableName).some(item => 
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

        for (const dbCacheFunc of allCacheFuncs) {
            const existsCache = this.fs.files.some(file => 
                file.content.cache.some(cache => {

                    const cacheTriggerFactory = new CacheTriggersBuilder(
                        cache,
                        this.database
                    );
                    const triggersByTableName = cacheTriggerFactory.createTriggers();
                
                    const existsSameCacheFunc = Object.values(triggersByTableName).some(item => 
                        item.function.equal( dbCacheFunc )
                    );
                    return existsSameCacheFunc;
                })
            );
            if ( !existsCache ) {
                this.migration.drop({
                    functions: [dbCacheFunc]
                });
            }
        }

        for (const table of this.database.tables) {
            for (const column of table.columns) {
                if ( !column.cacheSignature ) {
                    continue;
                }

                const existsCacheWithSameColumn = this.fs.files.some(file => 
                    file.content.cache.some(cache => {

                        if ( !cache.for.table.equal(table) ) {
                            return false;
                        }

                        const selectToUpdate = this.createSelectForUpdate(cache);
                        const hasSameNameColumn = selectToUpdate.columns.some(selectColumn =>
                            selectColumn.name === column.name
                        );
                        const newColumnType = this.getColumnType(
                            cache,
                            selectToUpdate
                        );
                        const hasSameType = newColumnType === column.type.toString();

                        return hasSameNameColumn && hasSameType;
                    })
                );

                if ( !existsCacheWithSameColumn ) {
                    this.migration.drop({
                        columns: [column]
                    });
                }
            }
        }
    }


    create() {
        const {sortedSelectsForEveryColumn} = this.createWithoutUpdates();
        this.updateAllColumns(
            sortedSelectsForEveryColumn
        );
    }

    createWithoutUpdates() {
        const allCache = flatMap(this.fs.files, file => file.content.cache);

        for (const cache of allCache) {
            const cacheTriggerFactory = new CacheTriggersBuilder(
                cache,
                this.database
            );
            const triggersByTableName = cacheTriggerFactory.createTriggers();
    
            for (const tableName in triggersByTableName) {
                const {trigger, function: func} = triggersByTableName[ tableName ];

                const table = this.database.getTable( trigger.table );
                const existsTrigger = table && table.triggers.some(existentTrigger =>
                    existentTrigger.equal( trigger )
                );
                const existFunc = this.database.functions.some(existentFunc =>
                    existentFunc.equal(func)
                );
    
                this.migration.create({
                    triggers: existsTrigger ? [] : [trigger],
                    functions: existFunc ? [] : [func]
                });
            }
        }

        const sortedSelectsForEveryColumn = this.sortSelectsByDependencies(allCache);
        
        this.createAllColumns(
            sortedSelectsForEveryColumn
        );

        return {sortedSelectsForEveryColumn};
    }

    private createAllColumns(sortedSelectsForEveryColumn: ISortSelectItem[]) {

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
                this.getColumnType(cache, select),
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

        for (let i = 0, n = sortedSelectsForEveryColumn.length; i < n; i++) {
            const {select, cache: cacheToCreate} = sortedSelectsForEveryColumn[ i ];

            const selectToUpdateOneTable: Select[] = [select];
            for (let j = i + 1; j < n; j++) {
                const nextItem = sortedSelectsForEveryColumn[ j ];
                if ( nextItem.cache !== cacheToCreate ) {
                    break;
                }

                selectToUpdateOneTable.push(nextItem.select);
            }

            const columnsToOnlyRequiredUpdate = selectToUpdateOneTable
                .map(select =>
                    select.columns[0] as SelectColumn
                )
                .filter(selectColumn => {
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
                });


            if ( !columnsToOnlyRequiredUpdate.length ) {
                continue;
            }

            const selectToUpdate = this.createSelectForUpdate(cacheToCreate)
                .cloneWith({
                    columns: columnsToOnlyRequiredUpdate
                });
            
            this.migration.create({
                updates: [{
                    select: selectToUpdate,
                    forTable: cacheToCreate.for
                }]
            });
        }
    }

    // TODO: make async and execute expression in db
    private getColumnType(cache: Cache, select: Select) {


        // TODO: detect default by expression
        // const columnsTypes = await this.postgres.getCacheColumnsTypes(
        //     select,
        //     cache.for
        // );

        // const columnType = Object.values(columnsTypes)[0];
        // return columnType;
        
        let {expression} = select.columns[0] as SelectColumn;
        let funcCall = expression.getFuncCalls()[0] as FuncCall;

        if ( funcCall.name === "coalesce" ) {
            expression = funcCall.args[0] as Expression;
            funcCall = expression.getFuncCalls()[0] as FuncCall;
            
            if ( !funcCall ) {
                return "text";
            }
        }

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
            const columnRef = firstArg.getColumnReferences()[0] as ColumnReference;

            if ( columnRef.name === "id" ) {
                return "integer[]";
            }

            const table = this.database.getTable(columnRef.tableReference.table);
            const column = table && table.getColumn(columnRef.name);
            
            if ( column ) {
                return column.type + "[]";
            }

            return "text[]";
        }
        if ( funcCall.name === "max" || funcCall.name === "min" ) {

            const firstArg = funcCall.args[0] as Expression;
            const columnRef = firstArg.getColumnReferences()[0] as ColumnReference;
            const table = this.database.getTable(columnRef.tableReference.table);
            const column = table && table.getColumn(columnRef.name);
            
            if ( column ) {
                return column.type.toString();
            }
        }

        return "text";
    }

    private getColumnDefault(select: Select) {
        const aggFactory = new AggFactory(
            select.columns[0] as SelectColumn
        );
        const aggregations = aggFactory.createAggregations();
        const agg = Object.values(aggregations)[0] as AbstractAgg;

        const defaultExpression = agg.default();
        return defaultExpression;
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