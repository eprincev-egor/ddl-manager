import { CacheColumnParams } from "../../Comparator/graph/CacheColumn";
import { CacheColumnGraph } from "../../Comparator/graph/CacheColumnGraph";
import {
    Expression,
    Cache,
    ColumnReference
} from "../../ast";
import { MAX_NAME_LENGTH } from "../../database/postgres/constants";
import { Database } from "../../database/schema/Database";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { createSelectForUpdate } from "../processor/createSelectForUpdate";
import { DatabaseTrigger } from "../../database/schema/DatabaseTrigger";
import { strict } from "assert";

export interface IReferenceMeta {
    columns: string[];
    expressions: Expression[];
    unknownExpressions: Expression[];
    filters: Expression[];
    cacheTableFilters: Expression[];
}

export class CacheContext {
    readonly graph: CacheColumnGraph;
    readonly cache: Cache;
    readonly allCacheForTriggerTable: Cache[];
    readonly allCacheForCacheTable: Cache[];
    readonly triggerTable: TableID;
    readonly triggerTableColumns: string[];
    readonly database: Database;
    readonly referenceMeta: IReferenceMeta;
    readonly excludeRef: TableReference | false
    
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
        const allCacheColumns: CacheColumnParams[] = [];

        for (const cache of this.allCacheForCacheTable) {
            const selectForUpdate = createSelectForUpdate(database, cache);

            for (const selectColumn of selectForUpdate.columns) {
                allCacheColumns.push({
                    for: cache.for,
                    name: selectColumn.name,
                    cache: {
                        name: cache.name,
                        signature: cache.getSignature()
                    },
                    select: selectForUpdate.cloneWith({
                        columns: [
                            selectColumn
                        ]
                    })
                })
            }
        }
        this.graph = new CacheColumnGraph(allCacheColumns);


        this.cache = cache;
        this.triggerTable = triggerTable;
        this.triggerTableColumns = triggerTableColumns;
        this.database = database;
        this.excludeRef = excludeRef ? cache.for : false;
        this.referenceMeta = this.buildReferenceMeta();
    }

    getDependencyLevel(columnName: string) {
        const column = this.getGraphColumn(columnName);
        return this.graph.getDependencyLevel(column);
    }

    getDependencyIndex(columnName: string) {
        const column = this.getGraphColumn(columnName);
        return this.graph.getDependencyIndex(column);
    }

    getTableReferencesToTriggerTable() {
        const tableReferences = this.cache.select.getAllTableReferences()
            .filter(tableRef =>
                tableRef.table.equal(this.triggerTable)
            );

        return tableReferences;
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

    getBeforeUpdateTriggers() {
        const triggerDbTable = this.database.getTable(this.triggerTable);

        const beforeUpdateTriggers = triggerDbTable?.triggers.filter(trigger =>
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
                from.table.equal(this.triggerTable)
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
                    join.table.equal(columnRef.tableReference.table)
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

    private getGraphColumn(columnName: string) {
        const column = this.graph.getAllColumns().find(column => 
            column.name == columnName
        );
    
        strict.ok(column, "unknown column: " + columnName);
        return column;
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