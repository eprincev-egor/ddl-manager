import { noReferenceChanges } from "./noReferenceChanges";
import { noChanges } from "./noChanges";
import { hasReference } from "./hasReference";
import { hasEffect } from "./hasEffect";
import { findJoinsMeta } from "../../processor/findJoinsMeta";
import { replaceArrayNotNullOn } from "./replaceArrayNotNullOn";
import { CacheContext } from "../CacheContext";
import {
    Expression,
    TableReference
} from "../../../ast";
import { replaceOperatorAnyToIndexedOperator } from "./replaceOperatorAnyToIndexedOperator";
import { replaceAmpArrayToAny } from "./replaceAmpArrayToAny";
import { flatMap } from "lodash";

export type RowType = "new" | "old";

export class ConditionBuilder {
    private readonly context: CacheContext;
    constructor(
        context: CacheContext
    ) {
        this.context = context;
    }

    build() {
        const joins = findJoinsMeta(this.context.cache.select);

        const mutableColumns = this.context.triggerTableColumns
            .filter(col => col !== "id");
        const mutableColumnsDepsInAggregations = mutableColumns
            .filter(col => 
                !this.context.referenceMeta.columns.includes(col)
            );
        
        const conditions = {

            hasMutableColumns: 
                mutableColumns.length > 0,
            hasMutableColumnsDepsInAggregations: 
                mutableColumnsDepsInAggregations.length > 0,
            
            noReferenceChanges: noReferenceChanges(
                this.context
            ),
            noChanges: noChanges(
                this.context
            ),
            hasOldEffect: hasEffect(
                this.context,
                "old",
                joins
            ),
            hasNewEffect: hasEffect(
                this.context,
                "new",
                joins
            )
        };
        return conditions;
    }

    getHasReference(row: RowType) {
        const hasReferenceCondition = hasReference(this.context);
        const output = this.replaceTriggerTableRefsTo(
            hasReferenceCondition,
            row
        );
        return output;
    }

    getNeedUpdateCondition(row: RowType) {
        const needUpdate = this.buildNeedUpdateCondition(row);
        const output = this.replaceTriggerTableRefsTo(needUpdate, row);
        return output;
    }

    getNeedUpdateConditionOnUpdate(row: RowType) {
        const needUpdate = replaceArrayNotNullOn(
            this.context,
            this.buildNeedUpdateCondition(row),
            arrayChangesFunc(row)
        );
        const output = this.replaceTriggerTableRefsTo(needUpdate, row);
        return output;
    }

    getSimpleWhere(row: RowType) {
        const simpleWhere = this.buildSimpleWhere();
        const output = this.replaceTriggerTableRefsTo(simpleWhere, row);
        return output;
    }

    getSimpleWhereOnUpdate(row: RowType) {
        const simpleWhere = this.buildSimpleWhere();
        const output = replaceArrayNotNullOn(
            this.context,
            this.replaceTriggerTableRefsTo(simpleWhere, row),
            arrayChangesFunc(row)
        );
        return output;
    }

    private buildSimpleWhere() {
        const conditions = this.context.referenceMeta.expressions.map(expression => {

            expression = replaceOperatorAnyToIndexedOperator(
                this.context.cache,
                expression
            );
            expression = replaceAmpArrayToAny(
                this.context.cache,
                expression
            );

            return expression;
        });

        const where = Expression.and(conditions);
        if ( !where.isEmpty() ) {
            return where;
        }
    }

    private buildNeedUpdateCondition(row: RowType) {
        const conditions = [
            hasReference(this.context),
            hasEffect(this.context, row, []),
            Expression.and(this.context.referenceMeta.filters),
            this.matchedAllAggFilters()
        ].filter(condition => 
            condition != null &&
            !condition.isEmpty()
        ) as Expression[];
    
        const needUpdate = Expression.and(conditions);
        if ( !needUpdate.isEmpty() ) {
            return needUpdate;
        }
    }
    
    private matchedAllAggFilters() {
    
        const allAggCalls = flatMap(
            this.context.cache.select.columns, 
            column => column.getAggregations()
        );
        const everyAggCallHaveFilter = allAggCalls.every(aggCall => aggCall.where != null);
        if ( !everyAggCallHaveFilter ) {
            return;
        }
    
        const filterConditions = allAggCalls.map(aggCall => {
            const expression = aggCall.where as Expression;
            return expression;
        });
    
        return Expression.or(filterConditions);
    }

    private replaceTriggerTableRefsTo(
        expression: Expression | undefined,
        row: "new" | "old"
    ) {
        if ( !expression ) {
            return;
        }
        let outputExpression = expression as Expression;

        const refsToTriggerTable = this.context.getTableReferencesToTriggerTable();

        refsToTriggerTable.forEach((triggerTableRef) => {

            outputExpression = outputExpression.replaceTable(
                triggerTableRef,
                new TableReference(
                    this.context.triggerTable,
                    row
                )
            );
        });

        return outputExpression;
    }
}

function arrayChangesFunc(row: RowType) {
    return row === "old" ? "cm_get_deleted_elements" : "cm_get_inserted_elements";
}