import { Expression } from "../../../ast";
import { CacheContext } from "../CacheContext";
import { TableReference } from "../../../database/schema/TableReference";
import { flatMap } from "lodash";

import { noReferenceChanges } from "./noReferenceChanges";
import { noChanges } from "./noChanges";
import { hasNoReference, hasReference } from "./hasReference";
import { hasEffect } from "./hasEffect";
import { findJoinsMeta } from "../../processor/findJoinsMeta";
import { replaceArrayNotNullOn } from "./replaceArrayNotNullOn";
import { replaceOperatorAnyToIndexedOperator } from "./replaceOperatorAnyToIndexedOperator";
import { replaceAmpArrayToAny } from "./replaceAmpArrayToAny";


export type RowType = "new" | "old";

export class ConditionBuilder {
    private readonly context: CacheContext;
    constructor(context: CacheContext) {
        this.context = context;
    }

    hasMutableColumns() {
        return this.getMutableColumns().length > 0;
    }

    noReferenceChanges() {
        return noReferenceChanges( this.context );
    }

    noChanges() {
        return noChanges(this.context);
    }

    hasEffect(row: RowType) {
        const joins = findJoinsMeta(this.context.cache.select);

        return hasEffect(
            this.context,
            row,
            joins
        );
    }

    hasReference(row: RowType) {
        const hasReferenceCondition = hasReference(this.context);
        const output = this.replaceTriggerTableRefsTo(
            hasReferenceCondition,
            row
        );
        return output;
    }

    hasNoReference(row: RowType) {
        const hasReferenceCondition = hasNoReference(this.context);
        const output = this.replaceTriggerTableRefsTo(
            hasReferenceCondition,
            row
        );
        return output;
    }

    needUpdateCondition(row: RowType) {
        const needUpdate = this.buildNeedUpdateCondition(row);
        const output = this.replaceTriggerTableRefsTo(needUpdate, row);
        return output;
    }

    needUpdateConditionOnUpdate(row: RowType) {
        const needUpdate = replaceArrayNotNullOn(
            this.context,
            this.buildNeedUpdateCondition(row),
            arrayChangesFunc(row)
        );
        const output = this.replaceTriggerTableRefsTo(needUpdate, row);
        return output;
    }

    simpleWhere(row: RowType) {
        const simpleWhere = this.buildSimpleWhere();
        const output = this.replaceTriggerTableRefsTo(simpleWhere, row);
        return output;
    }

    simpleWhereOnUpdate(row: RowType) {
        const simpleWhere = this.buildSimpleWhere();
        const output = replaceArrayNotNullOn(
            this.context,
            this.replaceTriggerTableRefsTo(simpleWhere, row),
            arrayChangesFunc(row)
        );
        return output;
    }

    private getMutableColumns() {
        const mutableColumns = this.context.triggerTableColumns
            .filter(col => col !== "id");
        return mutableColumns;
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
        row: RowType
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