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

            let expression = new Expression([
                agg.call
            ]);
            if ( agg.call.name === "sum" ) {
                expression = new Expression([
                    Expression.funcCall("coalesce", [
                        expression,
                        Expression.unknown( "0" )
                    ]),
                    UnknownExpressionElement.fromSql("::"),
                    UnknownExpressionElement.fromSql("numeric")
                ]);
            }

            const column = new SelectColumn({
                name: aggColumnName,
                expression
            });
            return column;
        });

        const needCreateMainColumn = (
            Object.keys(aggregations).length === 0
            ||
            !selectColumn.expression.isFuncCall()
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