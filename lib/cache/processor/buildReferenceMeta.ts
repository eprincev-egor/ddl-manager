import { Cache, Expression } from "../../ast";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";

export interface IReferenceMeta {
    columns: string[];
    expressions: Expression[];
    unknownExpressions: Expression[];
    filters: Expression[];
    cacheTableFilters: Expression[];
}

export function buildReferenceMeta(
    cache: Cache,
    triggerTable: TableID,
    excludeRef: TableReference | false = cache.for
): IReferenceMeta {

    const referenceMeta: IReferenceMeta = {
        columns: [],
        filters: [],
        expressions: [],
        unknownExpressions: [],
        cacheTableFilters: []
    };

    const where = cache.select.where;
    if ( !where ) {
        return referenceMeta;
    }

    for (let andCondition of where.splitBy("and")) {
        andCondition = andCondition.extrude();

        const conditionColumns = andCondition.getColumnReferences();

        const columnsFromCacheTable = conditionColumns.filter(columnRef =>
            columnRef.tableReference.equal(cache.for)
        );
        const columnsFromTriggerTable = conditionColumns.filter(columnRef =>
            columnRef.isRefTo(cache, triggerTable, excludeRef)
        );

        const fromTriggerTable = cache.select.from.find(from =>
            from.source.toString() === triggerTable.toString()
        );
        const leftJoinsOverTriggerTable = (fromTriggerTable || {joins: []}).joins
            .filter(join => 
                join.on.getColumnReferences()
                    .some(columnRef =>
                        columnRef.tableReference.table.equal(triggerTable)
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