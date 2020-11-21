import { Cache, Table, Expression } from "../../../ast";

export interface IReferenceMeta {
    columns: string[];
    expressions: Expression[];
    filters: Expression[];
}

export function buildReferenceMeta(
    cache: Cache,
    fromTable: Table
): IReferenceMeta {
    const referenceMeta: IReferenceMeta = {
        columns: [],
        filters: [],
        expressions: []
    };

    const where = cache.select.where;
    if ( !where ) {
        return referenceMeta;
    }

    for (const andCondition of where.splitBy("and")) {
        const conditionColumns = andCondition.getColumnReferences();

        const columnsFromCacheTable = conditionColumns.filter(columnRef =>
            columnRef.tableReference.equal(cache.for)
        );
        const columnsFromTriggerTable = conditionColumns.filter(columnRef =>
            columnRef.tableReference.table.equal(fromTable)
        );

        const isReference = (
            columnsFromCacheTable.length
            &&
            columnsFromTriggerTable.length
        );

        if ( isReference ) {
            referenceMeta.expressions.push(
                andCondition
            );
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
