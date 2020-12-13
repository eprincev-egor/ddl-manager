import { AbstractAgg, AggFactory } from "../aggregator";
import {
    Expression,
    SelectColumn,
    HardCode,
    SetItem,
    CaseWhen,
    Spaces
} from "../../ast";
import { flatMap } from "lodash";
import { createAggValue } from "../processor/createAggValue";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { CacheContext } from "../trigger-builder/CacheContext";
import { TableReference } from "../../database/schema/TableReference";

type AggType = "minus" | "plus";

export class SetItemsFactory {

    protected context: CacheContext;
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
        const aggFactory = new AggFactory(updateColumn);
        const aggMap = aggFactory.createAggregations();

        const setItems: SetItem[] = [];

        let updateExpression = updateColumn.expression;
        
        for (const columnName in aggMap) {
            const agg = aggMap[ columnName ];

            const sql = this.aggregate(
                aggType,
                agg,
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

    protected aggregate(
        aggType: AggType,
        agg: AbstractAgg,
        hasOtherUpdates: boolean = false
    ) {
        const triggerTable = this.context.triggerTable;
        const joins = findJoinsMeta(this.context.cache.select);

        let sql!: Expression;
    
        const value = createAggValue(
            triggerTable,
            joins,
            agg.call.args,
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
    
        if ( agg.call.where && hasOtherUpdates ) {
            const whenNeedUpdate = agg.call.where.replaceTable(
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
                else: Expression.unknown(agg.columnName)
            }) as any;
        }
    
    
        return sql;
    }
    
}

function aggType2row(aggType: AggType) {
    if ( aggType === "minus" ) {
        return "old";
    }
    else {
        return "new";
    }
}
