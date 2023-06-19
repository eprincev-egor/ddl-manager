import { CacheColumnParams } from "../../Comparator/graph/CacheColumn";
import { CacheColumnGraph } from "../../Comparator/graph/CacheColumnGraph";
import {
    Expression,
    Cache,
    ColumnReference,
    SelectColumn,
    Select
} from "../../ast";
import { MAX_NAME_LENGTH } from "../../database/postgres/constants";
import { Database } from "../../database/schema/Database";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { createSelectForUpdate } from "../processor/createSelectForUpdate";
import { DatabaseTrigger } from "../../database/schema/DatabaseTrigger";
import { leadingZero } from "./utils";
import { strict } from "assert";
import { groupBy } from "lodash";

export interface IReferenceMeta {
    columns: string[];
    expressions: Expression[];
    unknownExpressions: Expression[];
    filters: Expression[];
    cacheTableFilters: Expression[];
}

export class CacheContext {
    readonly cache: Cache;
    readonly allCacheForTriggerTable: Cache[];
    readonly allCacheForCacheTable: Cache[];
    readonly triggerTable: TableID;
    readonly triggerTableColumns: string[];
    readonly database: Database;
    readonly referenceMeta: IReferenceMeta;
    readonly excludeRef: TableReference | false
    private graph?: CacheColumnGraph;
    private selectForUpdate?: Select;
    
    constructor(
        allCache: Cache[],
        cache: Cache,

        // TODO: calculate it automatically
        triggerTable: TableID,
        triggerTableColumns: string[],

        database: Database,
        // TODO: split to two classes
        excludeRef: boolean = true
    ) {
        this.allCacheForTriggerTable = allCache.filter(someCache =>
            someCache.for.table.equal(triggerTable)
        );
        this.allCacheForCacheTable = allCache.filter(someCache =>
            someCache.for.table.equal(cache.for.table)
        );
        this.cache = cache;
        this.triggerTable = triggerTable;
        this.triggerTableColumns = triggerTableColumns;
        this.database = database;
        this.excludeRef = excludeRef ? cache.for : false;
        this.referenceMeta = this.buildReferenceMeta();
    }

    private getDependencyLevel(columnName: string) {
        const column = this.getGraphColumn(columnName);
        return this.getGraph().getDependencyLevel(column);
    }

    getDependencyIndex(columnName: string) {
        const column = this.getGraphColumn(columnName);
        return this.getGraph().getDependencyIndex(column);
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
        return columnRef.tableReference.table.equal(this.triggerTable) && (
            !this.excludeRef
            ||
            !columnRef.tableReference.equal(this.excludeRef)
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
        const dependencyIndexes = columns.map(column =>
            this.getDependencyIndex(column.name)
        );
        const minDependencyIndex = Math.min(...dependencyIndexes);

        let triggerName = [
            `cache${leadingZero(minDependencyIndex, 3)}`,
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
                `cache${leadingZero(minDependencyIndex, 3)}`,

                shortName(this.cache.name),

                shortName(tableRef && tableRef.alias || 
                    this.cache.for.table.name),

                postfix
            ].join("_")
        }

        return triggerName;
    }

    getBeforeUpdateTriggers() {
        const triggerDbTable = this.database.getTable(this.triggerTable) || {triggers: []};

        const beforeUpdateTriggers = triggerDbTable.triggers.filter(trigger =>
            trigger.before &&
            trigger.update
        );
        return beforeUpdateTriggers || [];
    }

    getTriggerFunction(trigger: DatabaseTrigger) {
        const dbFunction = this.database.functions.find(func =>
            func.name === trigger.procedure.name &&
            func.schema === trigger.procedure.schema
        );

        strict.ok(dbFunction);
        return dbFunction
    }

    createSelectForUpdate() {
        if ( !this.selectForUpdate ) {
            this.selectForUpdate = createSelectForUpdate(
                this.database,
                this.cache
            );
        }

        return this.selectForUpdate;
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
                    this.getDependencyIndex(columnA.name) >
                    this.getDependencyIndex(columnB.name) ? 1 : -1
                )
            })
        );
    }

    private buildReferenceMeta(): IReferenceMeta {

        const referenceMeta: IReferenceMeta = {
            columns: [],
            filters: [],
            expressions: [],
            unknownExpressions: [],
            cacheTableFilters: []
        };

        const where = this.cache.select.where;
        if ( !where ) {
            return referenceMeta;
        }

        for (let andCondition of where.splitBy("and")) {
            andCondition = andCondition.extrude();

            const conditionColumns = andCondition.getColumnReferences();

            const columnsFromCacheTable = conditionColumns.filter(columnRef =>
                columnRef.tableReference.equal(this.cache.for)
            );
            const columnsFromTriggerTable = conditionColumns.filter(columnRef =>
                this.isColumnRefToTriggerTable(columnRef)
            );

            const fromTriggerTable = this.cache.select.from.find(from =>
                from.source.toString() === this.triggerTable.toString()
            );
            const leftJoinsOverTriggerTable = (fromTriggerTable || {joins: []}).joins
                .filter(join => 
                    join.on.getColumnReferences()
                        .some(columnRef =>
                            columnRef.tableReference.table.equal(this.triggerTable)
                        )
                );
            
            const columnsFromTriggerTableOverLeftJoin = conditionColumns.filter(columnRef =>
                leftJoinsOverTriggerTable.some(join =>
                    join.getTable().equal(columnRef.tableReference.table)
                )
            );

            if ( columnsFromCacheTable.length === conditionColumns.length ) {
                referenceMeta.cacheTableFilters.push( andCondition );
            }

            const isReference = (
                columnsFromCacheTable.length
                &&
                columnsFromTriggerTable.length
            );

            if ( isReference ) {
                if ( isUnknownExpression(andCondition) ) {
                    referenceMeta.unknownExpressions.push(
                        andCondition
                    );
                }
                else {
                    referenceMeta.expressions.push(
                        andCondition
                    );
                }

                referenceMeta.columns.push(
                    ...columnsFromTriggerTable.map(columnRef =>
                        columnRef.name
                    )
                );
            }
            else if (
                columnsFromTriggerTableOverLeftJoin.length ||
                columnsFromTriggerTable.length 
            ) {
                referenceMeta.filters.push( andCondition );
            }
        }

        return referenceMeta;
    }

    withoutInsertCase(): boolean {
        return (this.cache.withoutInserts || []).includes(
            this.triggerTable.toString() 
        );
    }

    hasAgg() {
        return this.cache.hasAgg(this.database)
    }

    private getGraphColumn(columnName: string) {
        const column = this.getGraph().getAllColumns().find(column => 
            column.name == columnName
        );
    
        strict.ok(column, "unknown column: " + columnName);
        return column;
    }

    private getGraph() {
        if ( !this.graph ) {
            const allCacheColumns: CacheColumnParams[] = [];
            for (const cache of this.allCacheForCacheTable) {
                const selectForUpdate = createSelectForUpdate(this.database, cache);

                for (const selectColumn of selectForUpdate.columns) {
                    allCacheColumns.push({
                        for: cache.for,
                        name: selectColumn.name,
                        cache: {
                            name: cache.name,
                            signature: cache.getSignature()
                        },
                        select: selectForUpdate.clone({
                            columns: [
                                selectColumn
                            ]
                        })
                    })
                }
            }
            this.graph = new CacheColumnGraph(allCacheColumns);
        }

        return this.graph;
    }
}

function isUnknownExpression(expression: Expression): boolean {
    expression = expression.extrude();

    if ( expression.isBinary("=") ) {
        return false;
    }
    if ( expression.isBinary("&&") ) {
        return false;
    }
    if ( expression.isBinary("@>") ) {
        return false;
    }
    if ( expression.isBinary("<@") ) {
        return false;
    }
    if ( expression.isIn() ) {
        return false;
    }

    const orConditions = expression.splitBy("or");
    if ( orConditions.length > 1 ) {
        return orConditions.some(subExpression => 
            isUnknownExpression(subExpression)
        );
    }

    const andConditions = expression.splitBy("and");
    if ( andConditions.length > 1 ) {
        return andConditions.some(subExpression => 
            isUnknownExpression(subExpression)
        );
    }

    return true;
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