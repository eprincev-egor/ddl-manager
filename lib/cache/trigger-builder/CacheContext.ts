import {
    Expression,
    Cache
} from "../../ast";
import { Database } from "../../database/schema/Database";
import { TableID } from "../../database/schema/TableID";

export interface IReferenceMeta {
    columns: string[];
    expressions: Expression[];
    unknownExpressions: Expression[];
    filters: Expression[];
}

export class CacheContext {
    readonly cache: Cache;
    readonly triggerTable: TableID;
    readonly triggerTableColumns: string[];
    readonly database: Database;
    readonly referenceMeta: IReferenceMeta;
    
    constructor(
        cache: Cache,
        triggerTable: TableID,
        triggerTableColumns: string[],
        database: Database
    ) {
        this.cache = cache;
        this.triggerTable = triggerTable;
        this.triggerTableColumns = triggerTableColumns;
        this.database = database;
        this.referenceMeta = this.buildReferenceMeta();
    }

    getTableReferencesToTriggerTable() {
        const tableReferences = this.cache.select.getAllTableReferences()
            .filter(tableRef =>
                tableRef.table.equal(this.triggerTable)
            );

        return tableReferences;
    }

    private buildReferenceMeta(): IReferenceMeta {

        const referenceMeta: IReferenceMeta = {
            columns: [],
            filters: [],
            expressions: [],
            unknownExpressions: []
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
                columnRef.tableReference.table.equal(this.triggerTable) &&
                !columnRef.tableReference.equal(this.cache.for)
            );

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
            else {
                referenceMeta.filters.push( andCondition );
            }
        }

        return referenceMeta;
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