import {
    Expression,
    Cache,
    SelectColumn
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

            let expression = new Expression([
                agg.call
            ]);
            if ( agg.call.name === "sum" ) {
                expression = Expression.funcCall("coalesce", [
                    expression,
                    Expression.unknown( agg.default() )
                ]);
            }

            const column = new SelectColumn({
                name: aggColumnName,
                expression
            });
            return column;
        });

        if ( !selectColumn.expression.isFuncCall() ) {
            columns.push(selectColumn);
        }

        return columns;
    });

    const selectToUpdate = cache.select.cloneWith({
        columns: columnsToUpdate
    });
    return selectToUpdate;
}