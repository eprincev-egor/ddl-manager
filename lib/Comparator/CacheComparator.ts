import { AbstractComparator } from "./AbstractComparator";
import { Column } from "../database/schema/Column";
import { CacheTriggersBuilder, IOutputTrigger } from "../cache/CacheTriggersBuilder";
import { flatMap, uniq } from "lodash";
import { Migration } from "../Migrator/Migration";
import { IDatabaseDriver } from "../database/interface";
import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";
import { CacheColumn, CacheColumnParams } from "./graph/CacheColumn";
import { CacheColumnGraph } from "./graph/CacheColumnGraph";
import { CacheColumnBuilder } from "./CacheColumnBuilder";
import { Comment } from "../database/schema/Comment";

export class CacheComparator extends AbstractComparator {

    private graph: CacheColumnGraph;
    private columnBuilder: CacheColumnBuilder;
    private allCacheTriggers: IOutputTrigger[];

    constructor(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState,
        migration = Migration.empty()
    ) {
        super(driver, database, fs, migration);

        this.allCacheTriggers = [];

        const cacheColumns: CacheColumnParams[] = [];
        const allCache = this.fs.allCache();

        for (const cache of allCache) {
            const cacheTriggerFactory = new CacheTriggersBuilder(
                allCache, cache,
                this.database,
                this.fs
            );

            const cacheTriggers = cacheTriggerFactory.createTriggers();
    
            for (const trigger of cacheTriggers) {
                this.allCacheTriggers.push(trigger);
            }

            for (const toUpdate of cacheTriggerFactory.createSelectsForUpdate()) {
                for (const updateColumn of toUpdate.select.columns) {
                    cacheColumns.push({
                        for: toUpdate.for,
                        name: updateColumn.name,
                        cache: {
                            name: cache.name,
                            signature: cache.getSignature()
                        },
                        select: toUpdate.select.clone({
                            columns: [
                                updateColumn
                            ]
                        })
                    });
                }
            }
        }

        this.graph = new CacheColumnGraph(cacheColumns);

        this.columnBuilder = new CacheColumnBuilder({
            database: this.database,
            graph: this.graph,
            migration: this.migration,
            driver: this.driver
        })
    }

    async drop() {
        this.dropTrashTriggers();
        this.dropTrashFuncs();
        await this.dropTrashColumns();
    }

    async create() {
        this.createTriggers();
        await this.createColumns();
        this.updateColumns();
    }

    findChangedColumns() {
        const allCacheColumns = this.graph.getAllColumns();
        const changedColumns = allCacheColumns.filter(cacheColumn => {
            const table = this.database.getTable( cacheColumn.for.table );
            const dbColumn = table && table.getColumn( cacheColumn.name );
            if ( !dbColumn ) {
                return true;
            }

            const newComment = Comment.fromFs({
                objectType: "column",
                cacheSignature: cacheColumn.cache.signature,
                cacheSelect: cacheColumn.select.toString()
            });

            return !newComment.equal(dbColumn.comment);
        });
        return changedColumns;
    }

    async findBrokenColumns(params: {
        concreteTables?: string | string[]
    } = {}) {
        let allCacheColumns = this.findCacheColumnsForTables(params.concreteTables);

        const brokenColumns: CacheColumn[] = [];

        for (const column of allCacheColumns) {
            const columnRef = `${column.for.getIdentifier()}.${column.name}`;

            let whereBroken = `${columnRef} is distinct from tmp.${column.name}`

            const expression = column.select.columns[0].expression;
            if ( expression.isFuncCall() ) {
                const [call] = expression.getFuncCalls();
                if ( call.name === "array_agg" ) {
                    whereBroken = `
                        ${columnRef} is distinct from tmp.${column.name} and
                        not(${columnRef} @> tmp.${column.name} and
                        ${columnRef} <@ tmp.${column.name})
                    `
                }

                if ( call.name === "sum" ) {
                    whereBroken = `coalesce(${columnRef}, 0) is distinct from coalesce(tmp.${column.name}, 0)`
                }
            }

            column.select.columns[0].expression.isFuncCall()
            const selectHasBroken = `
                select exists(
                    select from ${column.for}
                    
                    left join lateral (
                        ${column.select.toSQL()}
                    ) as tmp on true
                    
                    where
                        ${whereBroken}
                ) as has_broken
            `;
            const {rows} = await this.driver.query(selectHasBroken);
            const isBroken = rows[0].has_broken;

            if ( isBroken ) {
                brokenColumns.push(column);
            }
        }
        
        return brokenColumns;
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
        await this.createColumns();
    }

    async refreshCache(concreteTables?: string) {
        await this.createColumns();

        if ( concreteTables ) {
            const concreteColumns = this.findCacheColumnsForTables(concreteTables);

            this.migration.create({
                updates: this.graph.generateUpdatesFor(concreteColumns)
            });
        }
        else {
            this.migration.create({
                updates: this.graph.generateAllUpdates()
            });
        }

        this.migration.enableCacheTriggersOnUpdate();
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
        const dbCacheColumns = flatMap(this.database.tables, table => table.columns)
            .filter(dbColumn => dbColumn.cacheSignature);

        for (const dbColumn of dbCacheColumns) {
            const existsCacheWithSameColumn = await this.existsCacheWithSameColumn(dbColumn);
            if ( !existsCacheWithSameColumn ) {
                this.migration.drop({
                    columns: [dbColumn]
                });
            }
        }
    }

    private async existsCacheWithSameColumn(column: Column) {
        const cacheColumn = this.graph.getColumns(column.table)
            .find(cacheColumn => column.equalName(cacheColumn));

        if ( cacheColumn ) {
            const newColumn = await this.columnBuilder.build(cacheColumn);
            return column.type.suit(newColumn.type);
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

        for (const trigger of this.fs.allTriggers()) {
            if ( !trigger.updateOf ) {
                continue;
            }

            const beforeUpdateTriggers = this.allCacheTriggers.filter(item =>
                item.trigger.before &&
                item.trigger.updateOf &&
                item.trigger.table.equal(trigger.table) &&
                item.function.findAssignColumns().some(column =>
                    trigger.updateOf!.includes(column)
                )
            );
            if ( beforeUpdateTriggers.length === 0 ) {
                continue;
            }

            const alsoNeedListenColumns = flatMap(beforeUpdateTriggers, ({trigger}) => 
                trigger.updateOf!
            );

            const fixedTrigger = trigger.clone({
                updateOf: uniq(
                    trigger.updateOf
                        .concat(alsoNeedListenColumns)
                        .sort()
                )
            });

            const dbTrigger = this.database
                .getTable(fixedTrigger.table)!
                .getTrigger(fixedTrigger.name);

            const existsSameInDb = dbTrigger && 
                dbTrigger.equal(fixedTrigger);


            if ( existsSameInDb ) {
                this.migration.unDropTrigger(trigger);
                this.migration.unCreateTrigger(trigger);
            } else {
                this.migration.drop({triggers: [trigger]});
                this.migration.create({triggers: [fixedTrigger]});
            }
        }
    }

    private async createColumns() {
        for (const cacheColumn of this.graph.getAllColumnsFromRootToDeps()) {
            const columnToCreate = await this.columnBuilder.build(cacheColumn);

            const table = this.database.getTable( cacheColumn.for.table );
            const existentColumn = table && table.getColumn(cacheColumn.name);
            const existsSameColumn = (
                existentColumn && 
                existentColumn.suit( columnToCreate )
            );

            if ( !existsSameColumn ) {
                this.migration.create({
                    columns: [columnToCreate]
                });
            }
        }

        this.recreateDepsTriggersToChangedColumns();
    }

    // fix: cannot drop column because other objects depend on it
    private recreateDepsTriggersToChangedColumns() {
        const allTriggers = flatMap(this.database.tables, table => table.triggers)
            .filter(dbTrigger => !dbTrigger.frozen)
            .filter(dbTrigger => !this.migration.toDrop.triggers
                .some(dropTrigger =>
                    dropTrigger.name === dbTrigger.name &&
                    dropTrigger.table.equal(dbTrigger.table)
            ));

        for (const dbTrigger of allTriggers) {
            const table = dbTrigger.table;
            const hasDepsToCacheColumn = (dbTrigger.updateOf || [])
                .some(triggerDepsColumnName => {
                    const thisColumnNeedDrop = this.migration.toDrop.columns
                        .some(columnToDrop =>
                            columnToDrop.equalName(triggerDepsColumnName) &&
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

    private updateColumns() {
        const changedColumns = this.graph.getAllColumnsFromRootToDeps().filter(column => {
            const existentTable = this.database.getTable(column.for.table);
            const existentColumn = existentTable && existentTable.getColumn(column.name);
            if ( !existentColumn ) {
                return true;
            }
            return existentColumn.comment.cacheSelect !== column.select.toString();
        });
        const requiredUpdates = this.graph.generateUpdatesFor(changedColumns);
        this.migration.create({
            updates: requiredUpdates
        });
    }

    private findCacheColumnsForTables(
        concreteTables?: string | string[]
    ) {
        if ( !concreteTables ) {
            return this.graph.getAllColumns();
        }

        const concreteColumns: CacheColumn[] = [];
    
        for (const table of String(concreteTables).split(/\s*,\s*/) ) {
            const tableColumns = this.graph.getColumns(table);
            concreteColumns.push(...tableColumns);
        }

        return concreteColumns;
    }
}
