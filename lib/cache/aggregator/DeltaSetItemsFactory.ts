import { AbstractAgg, AggFactory } from "../aggregator";
import {
    Expression,
    FuncCall,
    SelectColumn,
    HardCode,
    SetItem,
    NotExpression,
    CaseWhen,
    Spaces,
    ColumnReference,
    UnknownExpressionElement
} from "../../ast";
import { flatMap } from "lodash";
import { TableReference } from "../../database/schema/TableReference";
import { SetItemsFactory } from "./SetItemsFactory";
import { TableID } from "../../database/schema/TableID";

export class DeltaSetItemsFactory extends SetItemsFactory {

    delta() {
        const updateColumns = this.context.cache.select.columns.filter(updateColumn => {
            const aggregations = updateColumn.getAggregations(this.context.database);

            const isImmutable = aggregations.every(aggCall =>
                !aggCall.where &&
                isImmutableAggCall(aggCall)
            );
            const isOnlyCountAgg = (
                aggregations.length === 1 &&
                aggregations[0].name === "count" &&
                !aggregations[0].distinct &&
                !aggregations[0].where
            );

            return (
                !isImmutable &&
                !isOnlyCountAgg
            );
        });

        const setItems = flatMap(updateColumns, updateColumn => 
            this.createDeltaSetItemsByColumn(updateColumn)
        );
    
        return setItems;
    }

    private createDeltaSetItemsByColumn(updateColumn: SelectColumn): SetItem[] {

        const aggFactory = new AggFactory(this.context.database, updateColumn);
        const aggMap = aggFactory.createAggregations();

        const setItems: SetItem[] = [];

        let updateExpression = updateColumn.expression;
        
        for (const columnName in aggMap) {
            const agg = aggMap[ columnName ];

            // can be helper aggregation array_agg(id) for universal agg
            const isNoEffect = (
                !agg.call.where &&
                isImmutableAggCall(agg.call)
            );
            if ( isNoEffect ) {
                continue;
            }

            const sql = this.deltaAggregate( agg );

            const aggSetItem = new SetItem({
                column: columnName,
                value: sql
            });
            setItems.push(aggSetItem);

            updateExpression = updateExpression.replaceFuncCall(
                agg.call, `(${sql})`
            );
        }

        if ( !updateColumn.isAggCall( this.context.database ) ) {
            const mainSetItem = new SetItem({
                column: updateColumn.name,
                value: new HardCode({
                    sql: updateExpression
                    .toString()
                    .split("\n")
                })
            });
            setItems.push(mainSetItem);
        }

        return setItems;
    }

    private deltaAggregate(agg: AbstractAgg) {
        let sql!: Expression;

        const prevValue = this.replaceTriggerTableToRow(
            agg.call.args[0],
            "old"
        );
        const nextValue = this.replaceTriggerTableToRow(
            agg.call.args[0],
            "new"
        );

        sql = this.aggDelta(
            agg,
            prevValue,
            nextValue
        );

        if ( agg.call.where ) {
            // replace also and joins
            const matchedOld = this.replaceTriggerTableToRow(agg.call.where, "old");
            const matchedNew = this.replaceTriggerTableToRow(agg.call.where, "new");

            const needPlus = Expression.and([
                matchedNew,
                new NotExpression(matchedOld)
            ]);
            const needMinus = Expression.and([
                new NotExpression(matchedNew),
                matchedOld
            ]);
            const caseWhen = new CaseWhen({
                cases: [
                    {
                        when: needPlus,
                        then: this.aggregate("plus", agg)
                    },
                    {
                        when: needMinus,
                        then: this.aggregate("minus", agg)
                    },
                    ...(
                        isImmutableAggCall(agg.call) ? [] : [{
                            when: matchedNew,
                            then: sql as any
                        }]
                    )
                ],
                else: Expression.unknown(agg.columnName)
            });
            
            sql = caseWhen as any;
        }

        return sql;
    }

    private aggDelta(
        agg: AbstractAgg,
        prevValue: Expression,
        nextValue: Expression
    ) {
        const minus = agg.minus(
            Expression.unknown(agg.columnName),
            prevValue
        );
        let delta = agg.plus(minus, nextValue);

        const helpersAgg = agg.helpersAgg || [];
        for (const helperAgg of helpersAgg ) {
            
            if ( isImmutableAggCall(helperAgg.call) ) {
                continue;
            }

            const helperPrevValue = this.replaceTriggerTableToRow(
                helperAgg.call.args[0],
                "old"
            );
            const helperNextValue = this.replaceTriggerTableToRow(
                helperAgg.call.args[0],
                "new"
            );

            const helperMinus = helperAgg.minus(
                Expression.unknown(helperAgg.columnName),
                helperPrevValue
            );
            const helperDelta = helperAgg.plus(helperMinus, helperNextValue);
            const helperDeltaSQL = helperDelta
                .toSQL( Spaces.level(2) )
                .trim();

            delta = delta.replaceColumn(
                new ColumnReference(
                    new TableReference(
                        new TableID("", "")
                    ),
                    helperAgg.columnName
                ),
                UnknownExpressionElement.fromSql(helperDeltaSQL)
            );
        }

        return delta;
    }

}

function isImmutableAggCall(aggCall: FuncCall) {
    const columnReferences = aggCall.name === "count" ? 
        aggCall.args[0].getColumnReferences() : 
        aggCall.getColumnReferences();

    const onlyImmutableColumns = columnReferences.every(columnRef =>
        columnRef.name === "id"
    );
    return onlyImmutableColumns;
}