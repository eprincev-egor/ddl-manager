import { CacheColumnGraph } from "../../Comparator/graph/CacheColumnGraph";
import {
    Cache,
    ColumnReference,
    SelectColumn
} from "../../ast";
import { MAX_NAME_LENGTH } from "../../database/postgres/constants";
import { Database } from "../../database/schema/Database";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { DatabaseTrigger } from "../../database/schema/DatabaseTrigger";
import { leadingZero } from "./utils";
import { groupBy } from "lodash";
import { FilesState } from "../../fs/FilesState";
import { buildReferenceMeta, IReferenceMeta } from "../processor/buildReferenceMeta";
import { strict } from "assert";

export class CacheContext {
    readonly cache: Cache;
    readonly triggerTable: TableID;
    readonly triggerTableColumns: string[];
    readonly database: Database;
    readonly referenceMeta: IReferenceMeta;
    private excludeRef: TableReference | false
    private fs: FilesState;
    private graph: CacheColumnGraph;
    
    constructor(
        cache: Cache,

        // TODO: calculate it automatically
        triggerTable: TableID,
        triggerTableColumns: string[],

        database: Database,
        fs: FilesState,
        graph: CacheColumnGraph,

        // TODO: split to two classes
        excludeRef: boolean = true
    ) {
        this.cache = cache;
        this.triggerTable = triggerTable;
        this.triggerTableColumns = triggerTableColumns;
        this.database = database;
        this.fs = fs;
        this.excludeRef = excludeRef ? cache.for : false;

        this.referenceMeta = buildReferenceMeta(
            cache, triggerTable,
            this.excludeRef
        );
        this.graph = graph;
    }

    getAllCacheForTriggerTable() {
        return this.fs.getCachesForTable(this.triggerTable);
    }

    getDependencyLevel(columnName: string) {
        const column = this.getGraphColumn(columnName);
        return this.graph.getDependencyLevel(column);
    }

    getTableReferencesToTriggerTable() {
        const tableReferences = this.cache.select.getAllTableReferences()
            .filter(tableRef =>
                tableRef.table.equal(this.triggerTable)
            );

        return tableReferences;
    }

    newRow() {
        const newRow = new TableReference(
            this.cache.for.table,
            "new"
        );
        return newRow;
    }

    isColumnRefToTriggerTable(columnRef: ColumnReference) {
        return columnRef.isRefTo(
            this.cache, this.triggerTable,
            this.excludeRef
        );
    }

    generateTriggerName(postfix?: string) {
        const defaultTriggerName = [
            "cache",
            this.cache.name,
            "for",
            this.excludeRef ?
                this.cache.for.table.name :
                "self",
            postfix,
            "on",
            this.triggerTable.name
        ].filter(value => !!value).join("_");

        if ( defaultTriggerName.length >= MAX_NAME_LENGTH ) {
            const tableRef = this.cache.select
                .getAllTableReferences()
                .find(fromTableRef =>
                    fromTableRef.table.equal(this.triggerTable)
                );

            if ( tableRef && tableRef.alias ) {
                const shortTriggerName = [
                    "cache",
                    this.cache.name,
                    "for",
                    this.excludeRef ?
                        this.cache.for.table.name :
                        "self",
                    "on",
                    tableRef.alias
                ].join("_");
                return shortTriggerName;
            }
        }

        return defaultTriggerName;
    }

    generateOrderedTriggerName(
        columns: SelectColumn[],
        postfix: string
    ) {
        const dependencyLevels = columns.map(column =>
            this.getDependencyLevel(column.name)
        );
        const minDependencyLevel = Math.min(...dependencyLevels);

        let triggerName = [
            `cache${leadingZero(minDependencyLevel, 3)}`,
            this.cache.name,
            "for",
            this.cache.for.table.name,
            postfix
        ].join("_");
        
        if ( triggerName.length > MAX_NAME_LENGTH  ) {
            const tableRef = this.cache.select
                .getAllTableReferences()
                .find(fromTableRef =>
                    fromTableRef.table.equal(this.triggerTable)
                );

            triggerName = [
                `cache${leadingZero(minDependencyLevel, 3)}`,

                shortName(this.cache.name),

                shortName(tableRef && tableRef.alias || 
                    this.cache.for.table.name),

                postfix
            ].join("_")
        }

        return triggerName;
    }

    createSelectForUpdate() {
        return this.cache.createSelectForUpdate(
            this.database.aggregators
        );
    }

    createSelectForUpdateNewRow() {
        return this.createSelectForUpdate()
            .replaceTable(this.cache.for, this.newRow());
    }

    groupBySelectsForUpdateByLevel() {
        const selectValues = this.createSelectForUpdateNewRow();
        const columnsByLevel = groupBy(selectValues.columns, column => 
            this.getDependencyLevel(column.name)
        );
        const levels = Object.keys(columnsByLevel).sort((lvlA, lvlB) => 
            +lvlA - +lvlB
        );

        return levels.map(level => 
            selectValues.clone({
                columns: columnsByLevel[level].sort((columnA, columnB) =>
                    columnA.name > columnB.name ? 1 : -1
                )
            })
        );
    }

    withoutInsertCase(): boolean {
        return (this.cache.withoutInserts || []).includes(
            this.triggerTable.toString() 
        );
    }

    hasAgg() {
        return this.cache.hasAgg(this.database.aggregators)
    }

    private getGraphColumn(columnName: string) {
        const column = this.graph.getColumn(this.cache.for, columnName);
    
        strict.ok(column, "unknown column: " + columnName);
        return column;
    }
}

function shortName(longName: string) {
    const halfLength = Math.min(
        Math.floor(longName.length / 2),
        10
    );

    const leftSide = longName.slice(0, halfLength);
    const rightSide = longName.slice(-halfLength);

    return leftSide + rightSide;
}