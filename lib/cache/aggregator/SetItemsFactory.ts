import { AbstractAgg, AggFactory } from "../aggregator";
import {
    Expression,
    SelectColumn,
    HardCode,
    SetItem,
    CaseWhen,
    Spaces,
    UnknownExpressionElement,
    ColumnReference
} from "../../ast";
import { flatMap } from "lodash";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { CacheContext } from "../trigger-builder/CacheContext";
import { TableReference } from "../../database/schema/TableReference";
import { buildJoinVariables } from "../processor/buildJoinVariables";
import { TableID } from "../../database/schema/TableID";

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
        const aggFactory = new AggFactory(
            this.context.database,
            updateColumn
        );
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
        let sql!: Expression;
    
        const value = this.replaceTriggerTableToRow(
            agg.call.args[0],
            aggType2row(aggType)
        );
        sql = agg[aggType](Expression.unknown(agg.columnName), value);
    
        const helpersAgg = agg.helpersAgg || [];
        for (const helperAgg of helpersAgg) {
    
            const helperPrevValue = this.replaceTriggerTableToRow(
                helperAgg.call.args[0],
                aggType2row(aggType)
            );
    
            const helperValue = helperAgg[aggType](
                Expression.unknown(helperAgg.columnName),
                helperPrevValue
            );
            const helperSpaces = Spaces.level(2);
            const helperValueSQL = helperValue.toSQL(helperSpaces).trim();
    
            sql = sql.replaceColumn(
                new ColumnReference(
                    new TableReference(
                        new TableID("", "")
                    ),
                    helperAgg.columnName
                ),
                UnknownExpressionElement.fromSql(helperValueSQL)
            );
        }
    
        if ( agg.call.where && hasOtherUpdates ) {
            const whenNeedUpdate = this.replaceTriggerTableToRow(
                agg.call.where,
                aggType2row(aggType)
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

    protected replaceTriggerTableToRow(
        valueExpression: Expression,
        row: "new" | "old"
    ): Expression {
        const triggerTable = this.context.triggerTable;
        const joinsMeta = findJoinsMeta(this.context.cache.select);

        if ( joinsMeta.length ) {
            const joins = buildJoinVariables(
                this.context.database,
                joinsMeta,
                row
            );
            
            joins.forEach((join) => {
                valueExpression = valueExpression.replaceColumn(
                    join.table.column,
                    UnknownExpressionElement.fromSql(join.variable.name)
                );
            });
        }

        valueExpression = valueExpression.replaceTable(
            triggerTable,
            new TableReference(
                triggerTable,
                row
            )
        );
        return valueExpression;
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
