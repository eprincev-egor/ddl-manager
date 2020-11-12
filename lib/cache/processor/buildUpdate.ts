import { AbstractAgg, AggFactory } from "../aggregator";
import {
    AbstractAstElement,
    Cache,
    Table,
    Expression,
    FuncCall,
    SelectColumn,
    TableReference,
    HardCode,
    SetItem,
    Update,
    NotExpression,
    CaseWhen
} from "../../ast";
import { flatMap } from "lodash";
import { createAggValue } from "./createAggValue";
import { IJoinMeta } from "./findJoinsMeta";

type AggType = "minus" | "plus" | "delta";

export function buildUpdate(
    cache: Cache,
    triggerTable: Table,
    where: Expression | undefined,
    joins: IJoinMeta[],
    aggType: AggType
) {
    const update = new Update({
        table: cache.for.toString(),
        set: createSetItems(
            cache,
            triggerTable,
            joins,
            aggType
        ),
        where
    });
    return update;
}

function createSetItems(
    cache: Cache,
    triggerTable: Table,
    joins: IJoinMeta[],
    aggType: AggType
) {
    const updateColumns = prepareUpdateColumns(cache, aggType);
    const setItems = flatMap(updateColumns, updateColumn => 
        createSetItemsByColumn(
            triggerTable, 
            joins,
            updateColumn,
            aggType,
            updateColumns.length > 1
        )
    );

    return setItems;
}

function prepareUpdateColumns(cache: Cache, aggType: AggType) {
    let updateColumns = cache.select.columns;

    if ( aggType !== "delta" ) {
        return updateColumns;
    }

    updateColumns = updateColumns.filter(updateColumn => {
        const aggregations = updateColumn.getAggregations();

        const isImmutable = aggregations.every(aggCall =>
            !aggCall.where &&
            isImmutableAggCall(aggCall)
        );
        const isOnlyCountAgg = (
            aggregations.length === 1 &&
            aggregations[0].name === "count"
        );

        return (
            !isImmutable &&
            !isOnlyCountAgg
        );
    });

    return updateColumns;
}

function createSetItemsByColumn(
    triggerTable: Table,
    joins: IJoinMeta[],
    updateColumn: SelectColumn,
    aggType: AggType,
    hasOtherColumns: boolean
): SetItem[] {

    const aggFactory = new AggFactory(updateColumn);
    const aggMap = aggFactory.createAggregations();

    const setItems: SetItem[] = [];

    let updateExpression = updateColumn.expression;
    
    for (const columnName in aggMap) {
        const agg = aggMap[ columnName ];

        const sql = aggregate(
            triggerTable,
            joins,
            agg.call,
            aggType,
            agg,
            columnName,
            hasOtherColumns || !updateColumn.expression.isFuncCall()
        );

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

function aggregate(
    triggerTable: Table,
    joins: IJoinMeta[],
    aggCall: FuncCall,
    aggType: AggType,
    agg: AbstractAgg,
    aggColumnName: string,
    hasOtherUpdates: boolean
) {
    let sql!: AbstractAstElement;

    if ( aggType === "delta" ) {
        const prevValue = createAggValue(triggerTable, joins, aggCall.args, "old");
        const nextValue = createAggValue(triggerTable, joins, aggCall.args, "new");

        if ( aggCall.where ) {
            const matchedOld = aggCall.where.replaceTable(
                triggerTable,
                new TableReference(
                    triggerTable,
                    "old"
                )
            );
            const matchedNew = aggCall.where.replaceTable(
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
                        then: agg.plus(nextValue) as any
                    },
                    {
                        when: needMinus,
                        then: agg.minus(prevValue)
                    }
                ], 
                else: isImmutableAggCall(aggCall) ? 
                    Expression.unknown(aggColumnName) :
                    agg.delta(prevValue, nextValue) as any
            });
            
            sql = caseWhen;
        } else {
            sql = agg.delta(prevValue, nextValue);
        }
    }
    else {
        const value = createAggValue(
            triggerTable,
            joins,
            aggCall.args,
            aggType2row(aggType)
        );
        sql = agg[aggType](value);

        if ( aggCall.where && hasOtherUpdates ) {
            const whenNeedUpdate = aggCall.where.replaceTable(
                triggerTable,
                new TableReference(
                    triggerTable,
                    aggType2row(aggType)
                )
            );

            sql = new CaseWhen({
                cases: [
                    {
                        when: whenNeedUpdate,
                        then: sql as any
                    }
                ],
                else: Expression.unknown(aggColumnName)
            });
        }
    
    }

    return sql;
}

function aggType2row(aggType: AggType) {
    if ( aggType === "minus" ) {
        return "old";
    }
    else {
        return "new";
    }
}

function isImmutableAggCall(aggCall: FuncCall) {
    const columnReferences = aggCall.args[0].getColumnReferences();
    const onlyImmutableColumns = columnReferences.every(columnRef =>
        columnRef.name === "id"
    );
    return onlyImmutableColumns;
}