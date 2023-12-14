import { AbstractComparator } from "./AbstractComparator";
import { Column } from "../database/schema/Column";
import { CacheTriggersBuilder, IOutputTrigger } from "../cache/CacheTriggersBuilder";
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
        this.dropTrashColumns();
    }

    async create() {
        await this.createColumns();
        this.updateColumns();
    }

    getAllCacheTriggers() {
        return this.allCacheTriggers;
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

    private async createColumns() {
        for (const cacheColumn of this.graph.getAllColumnsFromRootToDeps()) {
            await this.createColumn(cacheColumn);
        }
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
