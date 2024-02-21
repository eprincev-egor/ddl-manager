import { Expression, NotExpression } from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildSelfUpdateByOtherTablesBody } from "./body/buildSelfUpdateByOtherTablesBody";
import { buildSelfAssignBeforeInsertByOtherTablesBody } from "./body/buildSelfAssignBeforeInsertByOtherTablesBody";

export class SelfUpdateByOtherTablesTriggerBuilder 
extends AbstractTriggerBuilder {

    createTriggers() {
        return [
            ...this.createOnInsertTriggers(),
            ...this.createOnUpdateTriggers(), 
        ];
    }

    private createOnInsertTriggers() {
        if ( !this.needInsertCase() ) {
            return [];
        }

        const selects = this.context.groupBySelectsForUpdateByLevel();
        return selects.map(select => {
            const triggerName = this.context.generateOrderedTriggerName(
                select.columns, "bef_ins"
            );
            return {
                trigger: this.createDatabaseTrigger({
                    name: triggerName,
                    before: true,
                    insert: true,
                }),
                procedure: this.createDatabaseFunction(
                    buildSelfAssignBeforeInsertByOtherTablesBody(
                        select,
                        this.buildNotMatchedConditions().notMatchedFilterOnInsert
                    ),
                    triggerName
                )
            }
        })
    }

    private createOnUpdateTriggers() {
        const updateOfColumns = this.buildUpdateOfColumns();
        if ( updateOfColumns.length === 0 ) {
            return [];
        }

        const selects = this.context.groupBySelectsForUpdateByLevel();
        return selects.map(select => {
            const triggerName = this.context.generateOrderedTriggerName(
                select.columns, "bef_upd"
            );
            return {
                // TODO: filter updateOf columns by select
                trigger: this.createDatabaseTrigger({
                    name: triggerName,
                    before: true,
                    update: true,
                    updateOf: this.buildUpdateOfColumns()
                }),
                procedure: this.createDatabaseFunction(
                    buildSelfUpdateByOtherTablesBody(
                        this.conditions.noChanges(),
                        select,
                        this.buildNotMatchedConditions().notMatchedFilterOnUpdate
                    ),
                    triggerName
                )
            }
        });
    }

    protected buildNotMatchedConditions() {
        if ( this.context.referenceMeta.cacheTableFilters.length ) {
            const notMatchedNew: NotExpression[] = [];
            const notMatchedOld: NotExpression[] = [];
            for (const filter of this.context.referenceMeta.cacheTableFilters) {
                const notFilterNew = new NotExpression(
                    this.replaceTriggerTableToRow("new", filter)
                );
                const notFilterOld = new NotExpression(
                    this.replaceTriggerTableToRow("old", filter)
                );
                notMatchedNew.push(notFilterNew);
                notMatchedOld.push(notFilterOld);
            }

            return { 
                notMatchedFilterOnInsert: Expression.or(notMatchedNew),
                notMatchedFilterOnUpdate:  Expression.and([
                    Expression.or(notMatchedOld),
                    Expression.or(notMatchedNew)
                ])
            }
        }

        return { 
            notMatchedFilterOnInsert: undefined,
            notMatchedFilterOnUpdate: undefined
        }
    }

    private needInsertCase(): boolean {
        if ( this.context.withoutInsertCase() ) {
            return false;
        }

        const condition = this.conditions.buildNoReference("new");
        if ( condition && !condition.isEmpty() ) {
            return true;
        }

        const isOnlySimpleAggregations = this.context.cache.select.columns.every(column =>{
            const funcs = column.expression.getFuncCalls();
            return (
                column.expression.isFuncCall() &&
                ["count", "sum", "avg", "min", "max", "string_agg", "array_agg"].includes(funcs[0].name)
            );
        });
        return !isOnlySimpleAggregations;
    }
}