import { AbstractExpressionElement, ColumnReference, Expression, NotExpression, UnknownExpressionElement } from "../../../ast";
import { CacheContext } from "../CacheContext";
import { TableReference } from "../../../database/schema/TableReference";
import { flatMap } from "lodash";
import { hasNoReference, hasReference } from "./hasReference";
import { replaceArrayNotNullOn } from "./replaceArrayNotNullOn";
import { replaceOperatorAnyToIndexedOperator } from "./replaceOperatorAnyToIndexedOperator";
import { replaceAmpArrayToAny } from "./replaceAmpArrayToAny";
import { findJoinsMeta } from "../../processor/findJoinsMeta";
import { buildJoinVariables } from "../../processor/buildJoinVariables";
import { Table } from "../../../database/schema/Table";
import { Column } from "../../../database/schema/Column";
import { buildArrVars } from "../../processor/buildArrVars";
import { CoalesceFalseExpression } from "../../../ast/expression/CoalesceFalseExpression";


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
        const importantColumnsRefs = this.triggerTableColumnsToRefs(
            this.context.referenceMeta.columns
        );

        for (const filter of this.context.referenceMeta.filters) {
            const filterColumnsRefs = filter.getColumnReferences();
            importantColumnsRefs.push( ...filterColumnsRefs );
        }

        return this.buildNoChanges( importantColumnsRefs );
    }

    noChanges() {
        const triggerTableColumnsRefs = this.triggerTableColumnsToRefs(
            this.context.triggerTableColumns
        );

        return this.buildNoChanges( triggerTableColumnsRefs );
    }

    hasNoReference(row: string) {
        const hasReferenceCondition = hasNoReference(this.context);
        const output = this.replaceTriggerTableRefsTo(
            hasReferenceCondition,
            row
        );
        return output;
    }

    hasReferenceWithoutJoins(row: string) {
        const needUpdate = this.buildHasReferenceWithoutJoins();
        const output = this.replaceTriggerTableRefsTo(needUpdate, row);
        return output;
    }

    filtersWithJoins(row: string) {
        let conditions: Expression[] = this.context.referenceMeta.filters.slice();

        const aggFilters = this.matchedAllAggFilters();
        if ( aggFilters ) {
            conditions.push(aggFilters);
        }
        
        conditions = conditions.filter(condition =>
            this.hasJoinsInside(condition)
        );

        conditions = conditions.map(condition =>
            this.replaceTriggerTableRefsTo(condition, row) as Expression
        );

        const output = conditions.length === 1 ? conditions[0] : Expression.and(conditions);
        if ( !output.isEmpty() ) {
            return output;
        }
    }

    needUpdateConditionOnUpdate(row: string, arrVarPrefix: string) {
        const needUpdate = replaceArrayNotNullOn(
            this.context,
            this.buildNeedUpdateConditionOnUpdate(),
            buildArrVars(this.context, arrVarPrefix)
        );
        const output = this.replaceTriggerTableRefsTo(needUpdate, row);
        return output;
    }

    simpleWhere(row: string) {
        const simpleWhere = this.buildSimpleWhere();
        const output = this.replaceTriggerTableRefsTo(simpleWhere, row);
        return output;
    }

    simpleWhereOnUpdate(row: string, arrVarPrefix: string) {
        const simpleWhere = this.buildSimpleWhere();
        const output = replaceArrayNotNullOn(
            this.context,
            this.replaceTriggerTableRefsTo(simpleWhere, row),
            buildArrVars(this.context, arrVarPrefix)
        );
        return output;
    }

    exitFromDeltaUpdateIf(): Expression | undefined {
        const conditions: (Expression | NotExpression)[] = this.context.referenceMeta.filters.map(filter =>
            new NotExpression(
                this.replaceTriggerTableRefsTo(filter, "new")!
            )
        );

        const hasNoReference = this.hasNoReference("new");
        if ( hasNoReference ) {
            conditions.unshift( hasNoReference );
        }

        if ( !conditions.length ) {
            return;
        }

        return Expression.or(conditions);
    }

    private buildNoChanges(columns: ColumnReference[]) {
        const mutableColumns = columns.filter(column =>
            column.name !== "id"
        );

        const conditions: string[] = [];
        for (const columnRef of mutableColumns) {

            const tableStructure = this.context.database.getTable(
                columnRef.tableReference.table
            ) as Table;
        
            const column = (
                tableStructure &&
                tableStructure.getColumn(columnRef.name)
            ) as Column;

            const columnRefExpression = new Expression([ columnRef ]);
            let oldColumn = this.replaceTriggerTableRefsTo(
                columnRefExpression,
                "old"
            ) as Expression;
            let newColumn = this.replaceTriggerTableRefsTo(
                columnRefExpression,
                "new"
            ) as Expression;

            oldColumn = oldColumn.replaceTable(
                this.context.triggerTable,
                new TableReference(
                    this.context.triggerTable,
                    "old"
                )
            );
            newColumn = newColumn.replaceTable(
                this.context.triggerTable,
                new TableReference(
                    this.context.triggerTable,
                    "new"
                )
            );
    
            if ( column && column.type.isArray() ) {
                conditions.push(`cm_equal_arrays(${newColumn}, ${oldColumn})`);
            }
            else {
                conditions.push(`${ newColumn } is not distinct from ${ oldColumn }`);
            }
        }
        
        const noChangesCondition = Expression.and(conditions);
        return noChangesCondition;
    }

    private getMutableColumns() {
        const mutableColumns = this.context.triggerTableColumns
            .filter(col => col !== "id");
        return mutableColumns;
    }

    private buildSimpleWhere() {
        const conditions = this.context.referenceMeta.expressions.map(expression => {

            // TODO: recursive
            const orExpressions = expression.extrude().splitBy("or").map(subExpression => {
                subExpression = subExpression.extrude();

                subExpression = replaceOperatorAnyToIndexedOperator(
                    this.context.cache,
                    this.context.database,
                    subExpression
                );
                subExpression = replaceAmpArrayToAny(
                    this.context.cache,
                    subExpression
                );

                return subExpression;
            });

            return Expression.or(orExpressions);
        });

        conditions.push(
            ...this.context.referenceMeta.unknownExpressions
        );

        conditions.push(
            ...this.context.referenceMeta.cacheTableFilters
        );

        const where = Expression.and(conditions);
        if ( !where.isEmpty() ) {
            return where;
        }
    }

    private buildNeedUpdateConditionOnUpdate() {
        const conditions = [
            hasReference(this.context),
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

    private buildHasReferenceWithoutJoins() {
        let conditions: Expression[] = [];

        const refCondition = hasReference(this.context);
        if ( refCondition ) {
            conditions.push(refCondition);
        }

        for (const where of this.context.referenceMeta.filters) {
            if ( !this.hasJoinsInside(where) ) {
                conditions.push(
                    where
                );
            }
        }

        const aggFilters = this.matchedAllAggFilters();
        if ( aggFilters && !this.hasJoinsInside(aggFilters) ) {
            conditions.push(
                aggFilters
            );
        }

        conditions = conditions.filter(condition => 
            condition != null &&
            !condition.isEmpty()
        );
    
        const needUpdate = Expression.and(conditions);
        if ( !needUpdate.isEmpty() ) {
            return needUpdate;
        }
    }

    private matchedAllAggFilters() {
    
        const allAggCalls = flatMap(
            this.context.cache.select.columns, 
            column => column.getAggregations(this.context.database)
        );
        const everyAggCallHaveFilter = allAggCalls.every(aggCall => aggCall.where != null);
        if ( !everyAggCallHaveFilter ) {
            return;
        }
    
        const filterConditions = allAggCalls.map(aggCall => {
            const expression = aggCall.where as Expression;
            return new CoalesceFalseExpression(expression);
        });
    
        return Expression.or(filterConditions);
    }

    replaceTriggerTableRefsTo(
        expression: AbstractExpressionElement | undefined,
        row: string
    ) {
        if ( !expression ) {
            return;
        }
        let outputExpression = expression as Expression;

        const refsToTriggerTable = this.context.getTableReferencesToTriggerTable();

        const joinsMeta = findJoinsMeta(this.context.cache.select);

        if ( joinsMeta.length ) {
            const joins = buildJoinVariables(
                this.context.database,
                joinsMeta,
                row
            );
            
            joins.forEach((join) => {
                outputExpression = outputExpression.replaceColumn(
                    join.table.column,
                    UnknownExpressionElement.fromSql(join.variable.name)
                );
            });
        }

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

    private hasJoinsInside(condition: Expression) {
        const joinsMeta = findJoinsMeta(this.context.cache.select);
        if ( !joinsMeta.length ) {
            return false;
        }

        const columnsRefs = condition.getColumnReferences();
        const hasJoins = columnsRefs.some(columnRef =>
            !columnRef.tableReference.table.equal(this.context.triggerTable)
        );
        return hasJoins;
    }

    private triggerTableColumnsToRefs(columnsNames: string[]) {
        const triggerTableRef = new TableReference( this.context.triggerTable );
        const triggerTableColumnsRefs = columnsNames.map(columnName =>
            new ColumnReference( triggerTableRef, columnName )
        );
        return triggerTableColumnsRefs;
    }
}
