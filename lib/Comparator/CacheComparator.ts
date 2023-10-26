import { AbstractComparator } from "./AbstractComparator";
import { Column } from "../database/schema/Column";
import { CacheTriggersBuilder, IOutputTrigger } from "../cache/CacheTriggersBuilder";
import { flatMap, uniq } from "lodash";
import { Migration } from "../Migrator/Migration";
import { IDatabaseDriver } from "../database/interface";
import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";
import { CacheColumn } from "./graph/CacheColumn";
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

        const allCache = this.fs.allCache();
        this.graph = CacheColumnGraph.build(database.aggregators, allCache);

        for (const cache of allCache) {
            const cacheTriggerFactory = new CacheTriggersBuilder(
                allCache, cache,
                this.database,
                this.graph,
                this.fs,
            );

            const cacheTriggers = cacheTriggerFactory.createTriggers();
    
            for (const trigger of cacheTriggers) {
                this.allCacheTriggers.push(trigger);
            }
        }

        this.columnBuilder = new CacheColumnBuilder({
            database: this.database,
            graph: this.graph,
            fs: this.fs,
            driver: this.driver
        })
    }

    async drop() {
        this.dropTrashTriggers();
        this.dropTrashFuncs();
        this.dropTrashColumns();
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

    async createLogFuncs() {
        for (const trigger of this.allCacheTriggers) {
            this.migration.create({
                functions: [trigger.function]
            });
        }
    }

    async refreshCache(targetTablesOrColumns?: string) {
        if ( targetTablesOrColumns ) {
            const concreteColumns = this.graph.findCacheColumnsForTablesOrColumns(targetTablesOrColumns);

            this.migration.create({
                updates: this.graph.generateUpdatesFor(concreteColumns)
            });
        }
        else {
            this.migration.create({
                updates: this.graph.generateAllUpdates()
            });
        }
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
                const [needDropTrigger] = this.database.getTriggersByProcedure(dbCacheFunc);
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

    private dropTrashColumns() {
        for (const dbColumn of this.database.getAllColumns()) {
            this.tryDropColumn(dbColumn)
        }
    }

    private tryDropColumn(dbColumn: Column) {
        const cacheColumn = this.graph.getColumn(dbColumn.table, dbColumn.name);
        if ( cacheColumn ) {
            return;
        }

        if ( !dbColumn.isFrozen() ) {
            this.migration.drop({
                columns: [dbColumn]
            });
            return;
        }

        const legacyInfo = dbColumn.comment.legacyInfo ?? {};

        if ( legacyInfo.type || "nulls" in legacyInfo ) {
            const oldDbColumn = dbColumn.clone({
                type: legacyInfo.type,
                nulls: legacyInfo.nulls
            });
            this.migration.create({
                columns: [oldDbColumn]
            });
        }
    }

    private createTriggers() {
        for (const {trigger, function: func} of this.allCacheTriggers) {
            
            const existFunc = this.database.getFunctions(func.name).some(existentFunc =>
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
            await this.createColumn(cacheColumn);
        }

        this.recreateDepsTriggersToChangedColumns();
    }

    private async createColumn(cacheColumn: CacheColumn) {
        const existentColumn = this.database.getColumn(
            cacheColumn.for.table,
            cacheColumn.name
        );
        const columnToCreate = await this.columnBuilder.build(cacheColumn);

        if ( existentColumn?.same( columnToCreate ) ) {
            return;
        }
        
        if ( existentColumn?.isFrozen() ) {
            columnToCreate.markColumnAsFrozen(existentColumn);
        }

        this.migration.create({
            columns: [columnToCreate]
        });
    }

    // fix: cannot drop column because other objects depend on it
    private recreateDepsTriggersToChangedColumns() {
        const allTriggers = flatMap(this.database.tables, table => table.triggers)
            .filter(dbTrigger => !this.migration.toDrop.triggers
                .some(dropTrigger =>
                    dropTrigger.name === dbTrigger.name &&
                    dropTrigger.table.equal(dbTrigger.table)
            ));

        for (const dbTrigger of allTriggers) {
            const table = dbTrigger.table;
            const hasDepsToCacheColumn = (dbTrigger.updateOf || [])
                .some(triggerDepsColumnName => {
                    const thisColumnNeedDrop = this.migration.toDrop.columns.some(columnToDrop =>
                        columnToDrop.equalName(triggerDepsColumnName) &&
                        columnToDrop.table.equal(table)
                    );
                    const thisColumnNeedChangeType = this.migration.toCreate.columns.some(columnToDrop =>
                        columnToDrop.equalName(triggerDepsColumnName) &&
                        columnToDrop.table.equal(table)
                    );
                    return thisColumnNeedDrop || thisColumnNeedChangeType;
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
            const existentColumn = existentTable?.getColumn(column.name);

            return (
                !existentColumn ||
                existentColumn.comment.cacheSelect !== column.select.toString()
            );
        });
        const requiredUpdates = this.graph.generateUpdatesFor(changedColumns);
        this.migration.create({
            updates: requiredUpdates
        });
    }
}
