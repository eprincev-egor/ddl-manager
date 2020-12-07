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

        for (const cacheTrigger of allCacheTriggers) {
            const existsCache = this.fs.files.some(file => 
                file.content.cache.some(cache =>
                    cache.getSignature() === cacheTrigger.cacheSignature
                )
            );
            if ( !existsCache ) {
                this.migration.drop({
                    triggers: [cacheTrigger]
                });
            }
        }

        for (const cacheFunc of allCacheFuncs) {
            const existsCache = this.fs.files.some(file => 
                file.content.cache.some(cache =>
                    cache.getSignature() === cacheFunc.cacheSignature
                )
            );
            if ( !existsCache ) {
                this.migration.drop({
                    functions: [cacheFunc]
                });
            }
        }

        for (const table of this.database.tables) {
            for (const column of table.columns) {
                if ( column.cacheSignature ) {
                    const existsCache = this.fs.files.some(file => 
                        file.content.cache.some(cache =>
                            cache.getSignature() === column.cacheSignature
                        )
                    );
                    if ( !existsCache ) {
                        this.migration.drop({
                            columns: [column]
                        });
                    }
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
    
                this.migration.create({
                    triggers: [trigger],
                    functions: [func]
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
            const column = new Column(
                cache.for.table,
                (select.columns[0] as SelectColumn).name,
                this.getColumnType(cache, select),
                this.getColumnDefault(select),
                Comment.fromFs({
                    objectType: "column",
                    cacheSignature: cache.getSignature()
                })
            );

            this.migration.create({
                columns: [column]
            });
        }
    }

    private updateAllColumns(sortedSelectsForEveryColumn: ISortSelectItem[]) {

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
                const cachesToDropOnThatTable = ([] as Cache[])/*this.migration.toDrop.cache*/.filter(cacheToDrop =>
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
            
            this.migration.create({
                updates: [{
                    select: selectToUpdate,
                    forTable: cacheToCreate.for
                }]
            });
        }
    }

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
            select,
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