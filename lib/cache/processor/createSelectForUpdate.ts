import {
    Expression,
    Cache,
    SelectColumn,
    ColumnReference,
    Select
} from "../../ast";
import { AggFactory } from "../aggregator";
import { flatMap } from "lodash";
import { Database } from "../../database/schema/Database";

export function createSelectForUpdate(
    database: Database,
    cache: Cache
) {

    const columnsToUpdate = flatMap(cache.select.columns, selectColumn => {
        const aggFactory = new AggFactory(database, selectColumn);
        const aggregations = aggFactory.createAggregations();
        
        const columns = Object.keys(aggregations).map(aggColumnName => {
            const agg = aggregations[ aggColumnName ];

            const column = new SelectColumn({
                name: aggColumnName,
                expression: new Expression([
                    agg.call
                ])
            });
            return column;
        });

        const needCreateMainColumn = (
            Object.keys(aggregations).length === 0
            ||
            !selectColumn.isAggCall( database )
        )
        if ( needCreateMainColumn ) {
            columns.push(selectColumn);
        }

        return columns;
    });

    let selectToUpdate = cache.select.cloneWith({
        columns: columnsToUpdate
    });
    if ( cache.select.orderBy ) {
        selectToUpdate = addHelperColumns(cache, selectToUpdate);
    }

    return selectToUpdate;
}

export function addHelperColumns(cache: Cache, select: Select) {
    const helperColumns = getOrderByColumnsRefs(select).map(columnRef =>
        new SelectColumn({
            name: helperColumnName(cache, columnRef.name),
            expression: new Expression([
                new ColumnReference(
                    select.from[0].table,
                    columnRef.name
                )
            ])
        })
    );

    return select.cloneWith({
        columns: [
            ...select.columns,
            ...helperColumns
        ]
    })
}

export function getOrderByColumnsRefs(select: Select) {
    const orderByColumns = select.orderBy!.getColumnReferences();
    
    const hasId = orderByColumns.some(columnRef => columnRef.name === "id");
    if ( !hasId ) {
        orderByColumns.unshift(
            new ColumnReference(
                select.from[0].table,
                "id"
            )
        );
    }

    return orderByColumns;
}

export function helperColumnName(cache: Cache, columnName: string) {
    return `__${cache.name}_${columnName}`;
}