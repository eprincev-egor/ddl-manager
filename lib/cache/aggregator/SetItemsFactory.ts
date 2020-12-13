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
import { findJoinsMeta, IJoinMeta } from "../processor/findJoinsMeta";
import { CacheContext } from "../trigger-builder/CacheContext";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";

type AggType = "minus" | "plus";

export class SetItemsFactory {

    private context: CacheContext;
    constructor(context: CacheContext) {
        this.context = context;
    }

    minus() {
        const updateColumns = this.context.cache.select.columns;
        const setItems = flatMap(updateColumns, updateColumn => 
            this.createSetItemsByColumn(
                updateColumn,
                "minus",
                updateColumns.length > 1
            )
        );
    
        return setItems;
    }

    plus() {
        const updateColumns = this.context.cache.select.columns;
        const setItems = flatMap(updateColumns, updateColumn => 
            this.createSetItemsByColumn(
                updateColumn,
                "plus",
                updateColumns.length > 1
            )
        );
    
        return setItems;
    }


    private createSetItemsByColumn(
        updateColumn: SelectColumn,
        aggType: AggType,
        hasOtherColumns: boolean
    ): SetItem[] {
        const triggerTable = this.context.triggerTable;
        const joins = findJoinsMeta(this.context.cache.select);

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

}

function aggregate(
    triggerTable: TableID,
    joins: IJoinMeta[],
    aggCall: FuncCall,
    aggType: AggType,
    agg: AbstractAgg,
    aggColumnName: string,
    hasOtherUpdates: boolean
) {
    let sql!: Expression;

    const value = createAggValue(
        triggerTable,
        joins,
        aggCall.args,
        aggType2row(aggType)
    );
    sql = agg[aggType](Expression.unknown(agg.columnName), value);

    const helpersAgg = agg.helpersAgg || [];
    for (const helperAgg of helpersAgg) {

        const helperPrevValue = createAggValue(
            triggerTable,
            joins,
            helperAgg.call.args,
            aggType2row(aggType)
        );

        const helperSpaces = Spaces.level(2);
        const helperValue = helperAgg[aggType](
            Expression.unknown(helperAgg.columnName),
            helperPrevValue
        )
            .toSQL(helperSpaces)
            .trim();

        sql = sql.replaceColumn(
            helperAgg.columnName.toString(),
            helperValue
        );
    }

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
        }) as any;
    }


    return sql;
}

function delta(
    triggerTable: TableID,
    joins: IJoinMeta[],
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