import {
    Expression,
    Cache,
    SelectColumn,
    UnknownExpressionElement
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

    const selectToUpdate = cache.select.cloneWith({
        columns: columnsToUpdate
    });
    return selectToUpdate;
}