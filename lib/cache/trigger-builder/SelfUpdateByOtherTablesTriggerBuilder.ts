import { Expression, NotExpression, Select } from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildSelfUpdateByOtherTablesBody } from "./body/buildSelfUpdateByOtherTablesBody";
import { buildSelfAssignBeforeInsertByOtherTablesBody } from "./body/buildSelfAssignBeforeInsertByOtherTablesBody";
import { leadingZero } from "./utils";
import { groupBy } from "lodash";

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

        const selects = this.buildSelects();
        return selects.map(select => {
            const triggerName = this.generateTriggerNameBySelect(
                select, "bef_ins"
            );
            return {
                trigger: this.createDatabaseTrigger({
                    name: triggerName,
                    after: false,
                    delete: false,
                    update: false,

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

        const selects = this.buildSelects();
        return selects.map(select => {
            const triggerName = this.generateTriggerNameBySelect(
                select, "bef_upd"
            );
            return {
                // TODO: filter updateOf columns by select
                trigger: this.createDatabaseTrigger({
                    name: triggerName,
                    after: false,
                    before: true,
                    insert: false,
                    delete: false,
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

    private buildSelects() {
        const selectValues = this.context.createSelectForUpdateNewRow();
        const columnsByLevel = groupBy(selectValues.columns, column => 
            this.context.getDependencyLevel(column.name)
        );
        const levels = Object.keys(columnsByLevel).sort((lvlA, lvlB) => 
            +lvlA - +lvlB
        );

        return levels.map(level => 
            selectValues.cloneWith({
                columns: columnsByLevel[level]
            })
        );
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

        const hasReference = this.conditions.hasNoReference("new");
        if ( !hasReference || hasReference.isEmpty() ) {
            return false;
        }

        return true;
    }

    private generateTriggerNameBySelect(
        select: Select,
        postfix: string
    ) {
        const dependencyIndexes = select.columns.map(column =>
            this.context.getDependencyIndex(column.name)
        );
        const minDependencyIndex = Math.min(...dependencyIndexes);

        const triggerName = [
            `cache${leadingZero(minDependencyIndex, 3)}`,
            this.context.cache.name,
            "for",
            this.context.cache.for.table.name,
            postfix
        ].join("_");
        return triggerName;
    }
}