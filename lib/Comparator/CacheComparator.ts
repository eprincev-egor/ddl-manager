import { AbstractComparator } from "./AbstractComparator";
import { Column } from "../database/schema/Column";
import { CacheTriggersBuilder, IOutputTrigger } from "../cache/CacheTriggersBuilder";
import { flatMap } from "lodash";
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
        migration: Migration
    ) {
        super(driver, database, fs, migration);

        this.allCacheTriggers = [];

        const cacheColumns: CacheColumnParams[] = [];

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
                    cacheColumns.push({
                        for: toUpdate.for,
                        name: updateColumn.name,
                        cache: {
                            name: cache.name,
                            signature: cache.getSignature()
                        },
                        select: toUpdate.select.cloneWith({
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

    async findBrokenColumns() {
        const allCacheColumns = this.graph.getAllColumns();
        const brokenColumns: CacheColumn[] = [];

        for (const column of allCacheColumns) {
            const selectHasBroken = `
                select exists(
                    select from ${column.for}
                    
                    left join lateral (
                        ${column.select.toSQL()}
                    ) as tmp on true
                    
                    where
                        ${column.for.getIdentifier()}.${column.name} is distinct from tmp.${column.name}
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
            const concreteColumns: CacheColumn[] = [];

            for (const table of concreteTables.split(/\s*,\s*/) ) {
                const tableColumns = this.graph.getColumns(table);
                concreteColumns.push(...tableColumns);
            }

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
            .find(cacheColumn => cacheColumn.name === column.name);

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

        this.recreateDepsTriggers();
    }

    // fix: cannot drop column because other objects depend on it
    private recreateDepsTriggers() {
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

}