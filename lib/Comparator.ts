import { Database } from "./database/schema/Database";
import { DatabaseTrigger } from "./database/schema/DatabaseTrigger";
import { DatabaseFunction } from "./database/schema/DatabaseFunction";
import { Column } from "./database/schema/Column";
import { FilesState } from "./fs/FilesState";
import { Migration } from "./Migrator/Migration";
import {
    Cache,
    Select,
    From,
    SelectColumn,
    FuncCall,
    Expression,
    ColumnReference 
} from "./ast";
import { CacheTriggersBuilder } from "./cache/CacheTriggersBuilder";
import { AbstractAgg, AggFactory } from "./cache/aggregator";
import { flatMap } from "lodash";
import {
    ISortSelectItem,
    sortSelectsByDependencies
} from "./Migrator/cache/graph-util";

export class Comparator {

    static compare(database: Database, fs: FilesState) {
        const comparator = new Comparator(database, fs);
        return comparator.compare();
    }

    private migration: Migration;
    private database: Database;
    private fs: FilesState;

    private constructor(database: Database, fs: FilesState) {
        this.database = database;
        this.fs = fs;
        this.migration = Migration.empty();
    }

    compare() {
        this.dropOldObjects();
        this.createNewObjects();

        return this.migration;
    }

    private dropOldObjects() {
        this.dropOldTriggers();
        this.dropOldFunctions();
        this.dropOldCache();
    }

    private dropOldTriggers() {
        for (const table of this.database.tables) {
            for (const dbTrigger of table.triggers) {
                
                if ( dbTrigger.frozen ) {
                    continue;
                }

                const existsSameTriggerFromFile = flatMap(this.fs.files, file => file.content.triggers).some(fileTrigger =>
                    fileTrigger.equal(dbTrigger)
                );

                if ( !existsSameTriggerFromFile ) {
                    this.migration.drop({
                        triggers: [dbTrigger]
                    });
                }
            }
        }
    }

    private dropOldFunctions() {
        for (const dbFunc of this.database.functions) {
            
            // ddl-manager cannot drop frozen function
            if ( dbFunc.frozen ) {
                continue;
            }

            const existsSameFuncFromFile = flatMap(this.fs.files, file => file.content.functions).some(fileFunc =>
                fileFunc.equal(dbFunc)
            );

            if ( existsSameFuncFromFile ) {
                continue;
            }

            // for drop function, need drop trigger, who call it function
            if ( dbFunc.returns.type === "trigger" ) {
                const depsTriggers = this.database.getTriggersByProcedure({
                    schema: dbFunc.schema,
                    name: dbFunc.name,
                    args: dbFunc.args.map(arg => arg.type)
                }).filter(dbTrigger => {
                    const isDepsTrigger = (
                        dbTrigger.procedure.schema === dbFunc.schema &&
                        dbTrigger.procedure.name === dbFunc.name
                    );

                    if ( !isDepsTrigger ) {
                        return false;
                    }
                    
                    const existsSameTriggerFromFile = flatMap(this.fs.files, file => file.content.triggers).some(fileTrigger =>
                        fileTrigger.equal(dbTrigger)
                    );

                    // if trigger has change, then he will dropped
                    // in next cycle
                    if ( !existsSameTriggerFromFile ) {
                        return false;
                    }

                    // we have trigger and he without changes
                    return true;
                });

                // drop
                this.migration.drop({triggers: depsTriggers});
                // and create again
                this.migration.create({triggers: depsTriggers});
            }
            
            
            this.migration.drop({
                functions: [dbFunc]
            });
        }
    }

    private dropOldCache() {
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
    }

    private createNewObjects() {
        for (const file of this.fs.files) {
            this.createNewFunctions( file.content.functions );
            this.createNewTriggers( file.content.triggers );
        }
        this.createCaches();
    }

    private createNewFunctions(functions: DatabaseFunction[]) {
        for (const func of functions) {
            const existsSameFuncFromDb = this.database.functions.find(dbFunc =>
                dbFunc.equal(func)
            );

            if ( existsSameFuncFromDb ) {
                continue;
            }

            this.migration.create({
                functions: [func]
            });
        }
    }

    private createNewTriggers(triggers: DatabaseTrigger[]) {
        for (const trigger of triggers) {

            const dbTable = this.database.getTable(trigger.table);

            const existsSameTriggerFromDb = dbTable && dbTable.triggers.some(dbTrigger =>
                dbTrigger.equal(trigger)
            );

            if ( existsSameTriggerFromDb ) {
                continue;
            }

            this.migration.create({
                triggers: [trigger]
            });
        }
    }

    private createCaches() {
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
        this.updateAllColumns(
            sortedSelectsForEveryColumn
        );
    }

    private async createAllColumns(sortedSelectsForEveryColumn: ISortSelectItem[]) {

        for (const {cache, select} of sortedSelectsForEveryColumn) {
            const column = new Column(
                cache.for.table,
                (select.columns[0] as SelectColumn).name,
                this.getColumnType(cache, select),
                this.getColumnDefault(select),

                `ddl-cache-signature(${ cache.getSignature() })`
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
