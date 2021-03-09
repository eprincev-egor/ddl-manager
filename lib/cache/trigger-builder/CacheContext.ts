import {
    Expression,
    Cache,
    ColumnReference
} from "../../ast";
import { MAX_NAME_LENGTH } from "../../database/postgres/constants";
import { Database } from "../../database/schema/Database";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";

export interface IReferenceMeta {
    columns: string[];
    expressions: Expression[];
    unknownExpressions: Expression[];
    filters: Expression[];
    cacheTableFilters: Expression[];
}

export class CacheContext {
    readonly cache: Cache;
    readonly triggerTable: TableID;
    readonly triggerTableColumns: string[];
    readonly database: Database;
    readonly referenceMeta: IReferenceMeta;
    readonly excludeRef: TableReference | false
    
    constructor(
        cache: Cache,
        triggerTable: TableID,
        triggerTableColumns: string[],
        database: Database,
        // TODO: split to two classes
        excludeRef: boolean = true
    ) {
        this.cache = cache;
        this.triggerTable = triggerTable;
        this.triggerTableColumns = triggerTableColumns;
        this.database = database;
        this.excludeRef = excludeRef ? cache.for : false;
        this.referenceMeta = this.buildReferenceMeta();
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

    generateTriggerName() {
        const defaultTriggerName = [
            "cache",
            this.cache.name,
            "for",
            this.excludeRef ?
                this.cache.for.table.name :
                "self",
            "on",
            this.triggerTable.name
        ].join("_");

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