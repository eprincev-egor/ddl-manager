import { AbstractAgg, AggFactory } from "../aggregator";
import {
    Expression,
    FuncCall,
    SelectColumn,
    HardCode,
    SetItem,
    NotExpression,
    CaseWhen,
    Spaces
} from "../../ast";
import { flatMap } from "lodash";
import { createAggValue } from "../processor/createAggValue";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { TableReference } from "../../database/schema/TableReference";
import { SetItemsFactory } from "./SetItemsFactory";

export class DeltaSetItemsFactory extends SetItemsFactory {

    delta() {
        const updateColumns = this.context.cache.select.columns.filter(updateColumn => {
            const aggregations = updateColumn.getAggregations();

            const isImmutable = aggregations.every(aggCall =>
                !aggCall.where &&
                isImmutableAggCall(aggCall)
            );
            const isOnlyCountAgg = (
                aggregations.length === 1 &&
                aggregations[0].name === "count" &&
                !aggregations[0].distinct
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

        const aggFactory = new AggFactory(updateColumn);
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

        if ( !updateColumn.expression.isFuncCall() ) {
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
        const triggerTable = this.context.triggerTable;
        const joins = findJoinsMeta(this.context.cache.select);

        let sql!: Expression;

        const prevValue = createAggValue(triggerTable, joins, agg.call.args, "old");
        const nextValue = createAggValue(triggerTable, joins, agg.call.args, "new");

        sql = this.aggDelta(
            agg,
            prevValue,
            nextValue
        );

        if ( agg.call.where ) {
            const matchedOld = agg.call.where.replaceTable(
                triggerTable,
                new TableReference(
                    triggerTable,
                    "old"
                )
            );
            const matchedNew = agg.call.where.replaceTable(
                triggerTable,
                new TableReference(
                    triggerTable,
                    "new"
                )
            );
            
            ;
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
                    }
                ], 
                else: isImmutableAggCall(agg.call) ? 
                    Expression.unknown(agg.columnName) :
                    sql as any
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
        const triggerTable = this.context.triggerTable;
        const joins = findJoinsMeta(this.context.cache.select);

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

            const helperPrevValue = createAggValue(
                triggerTable,
                joins,
                helperAgg.call.args,
                "old"
            );
            const helperNextValue = createAggValue(
                triggerTable,
                joins,
                helperAgg.call.args,
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
                helperAgg.columnName.toString(),
                helperDeltaSQL
            );
        }

        return delta;
    }

}

function isImmutableAggCall(aggCall: FuncCall) {
    const columnReferences = aggCall.args[0].getColumnReferences();
    const onlyImmutableColumns = columnReferences.every(columnRef =>
        columnRef.name === "id"
    );
    return onlyImmutableColumns;
}