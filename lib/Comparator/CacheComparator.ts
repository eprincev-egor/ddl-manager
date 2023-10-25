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
import { ColumnReference, Expression, From, Select, SelectColumn, UnknownExpressionElement } from "../ast";
import { TableReference } from "../database/schema/TableReference";
import { buildReferenceMeta } from "../cache/processor/buildReferenceMeta";

export interface IFindBrokenColumnsParams {
    timeout?: number;
    concreteTables?: string | string[];
    onStartScanColumn?: (column: string) => void;
    onScanColumn?: (result: IColumnScanResult) => void;
    onScanError?: (result: IColumnScanError) => void;
}

export interface IColumnScanResult {
    column: string;
    hasWrongValues: boolean;
    time: TimeRange;
}

export interface IColumnScanError {
    column: string;
    error: Error;
    time: TimeRange;
}

export interface TimeRange {
    start: Date;
    end: Date;
    duration: number;
}

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
            const selectForUpdate = cache.createSelectForUpdate(database.aggregators);

            for (const updateColumn of selectForUpdate.columns) {
                cacheColumns.push({
                    for: cache.for,
                    name: updateColumn.name,
                    cache: {
                        name: cache.name,
                        signature: cache.getSignature()
                    },
                    select: selectForUpdate.clone({
                        columns: [
                            updateColumn
                        ]
                    })
                });
            }

            const needLastRowColumn = (
                cache.select.from.length === 1 &&
                cache.select.orderBy &&
                cache.select.limit === 1
            );
            if ( needLastRowColumn ) {
                const fromRef = cache.select.getFromTable();
                const prevRef = new TableReference(
                    fromRef.table,
                    `prev_${ fromRef.table.name }`
                );
                const orderBy = cache.select.orderBy!.items[0]!;
                const columnName = cache.getIsLastColumnName();
                const referenceMeta = buildReferenceMeta(
                    cache, fromRef.table
                );

                cacheColumns.push({
                    for: fromRef,
                    name: columnName,
                    cache: {
                        name: cache.name,
                        signature: cache.getSignature()
                    },
                    select: Select.notExists({
                        from: [
                            new From({source: prevRef})
                        ],
                        where: Expression.and([
                            ...referenceMeta.columns.map(column =>
                                new Expression([
                                    new ColumnReference(prevRef, column),
                                    UnknownExpressionElement.fromSql("="),
                                    new ColumnReference(fromRef, column),
                                ])
                            ),
                            ...referenceMeta.filters.map(filter =>
                                filter.replaceTable(
                                    fromRef,
                                    prevRef
                                )
                            ),
                            new Expression([
                                new ColumnReference(prevRef, "id"),
                                UnknownExpressionElement.fromSql(
                                    orderBy.type === "desc" ? 
                                        ">" : "<"
                                ),
                                new ColumnReference(fromRef, "id"),
                            ])
                        ])
                    }, columnName)
                });
            }
        }

        this.graph = new CacheColumnGraph(cacheColumns);

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

    async findBrokenColumns(params: IFindBrokenColumnsParams = {}) {
        let allCacheColumns = this.findCacheColumnsForTablesOrColumns(params.concreteTables);

        const brokenColumns: CacheColumn[] = [];

        for (const column of allCacheColumns) {
            const hasWrongValues = await this.tryScanColumnOnWrongValues(params, column);

            if ( hasWrongValues ) {
                brokenColumns.push(column);
            }
        }
        
        return brokenColumns;
    }

    private async tryScanColumnOnWrongValues(
        params: IFindBrokenColumnsParams,
        column: CacheColumn
    ) {
        const timeStart = new Date();

        if ( params.onStartScanColumn ) {
            params.onStartScanColumn(column.toString());
        }

        try {
            const hasWrongValues = await this.scanColumnOnWrongValues(params, column);
            
            if ( params.onScanColumn ) {
                const timeEnd = new Date();

                params.onScanColumn({
                    column: column.toString(),
                    hasWrongValues,
                    time: {
                        start: timeStart,
                        end: timeEnd,
                        duration: +timeEnd - +timeStart
                    }
                });
            }

            return hasWrongValues;
        } catch(error) {
            if ( params.onScanError ) {
                const timeEnd = new Date();

                params.onScanError({
                    column: column.toString(),
                    error: error as any,
                    time: {
                        start: timeStart,
                        end: timeEnd,
                        duration: +timeEnd - +timeStart
                    }
                });
            }
        }
    }

    private async scanColumnOnWrongValues(
        params: IFindBrokenColumnsParams,
        column: CacheColumn
    ) {
        const columnRef = `${column.for.getIdentifier()}.${column.name}`;
        let whereBroken = `${columnRef} is distinct from tmp.${column.name}`

        const expression = column.getColumnExpression();
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

        if ( params.timeout ) {
            const {rows} = await this.driver.queryWithTimeout(
                selectHasBroken,
                params.timeout
            );
            return rows[0].has_broken;
        }
        const {rows} = await this.driver.query(selectHasBroken);

        return rows[0].has_broken;
    }

    async createLogFuncs() {
        for (const trigger of this.allCacheTriggers) {
            this.migration.create({
                functions: [trigger.function]
            });
        }
    }

    async refreshCache(targetTablesOrColumns?: string) {
        await this.createColumns();

        if ( targetTablesOrColumns ) {
            const concreteColumns = this.findCacheColumnsForTablesOrColumns(targetTablesOrColumns);

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

    private findCacheColumnsForTablesOrColumns(
        targetTablesOrColumns?: string | string[]
    ) {
        if ( !targetTablesOrColumns ) {
            return this.graph.getAllColumns();
        }

        const concreteColumns: CacheColumn[] = [];
    
        for (const tableOrColumn of String(targetTablesOrColumns).split(/\s*,\s*/) ) {
            const path = tableOrColumn.trim().toLowerCase().split(".");
            const tableId = path.slice(0, 2).join(".");
            const tableColumns = this.graph.getColumns(tableId);

            if ( path.length === 3 ) {
                const columnName = path.slice(-1)[0];
                const column = tableColumns.find(cacheColumn => 
                    cacheColumn.name === columnName
                );
                if ( column ) {
                    concreteColumns.push(column);
                }
            }
            else {
                concreteColumns.push(...tableColumns);
            }
        }

        return concreteColumns;
    }
}
