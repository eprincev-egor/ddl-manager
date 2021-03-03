import { Update, Expression, SetItem } from "../../ast";
import { CoalesceFalseExpression } from "../../ast/expression/CoalesceFalseExpression";
import { TableReference } from "../../database/schema/TableReference";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildOneRowBody } from "./body/buildOneRowBody";

type Row = "new" | "old";

export class OneRowTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const updateNew = new Update({
            table: this.context.cache.for.toString(),
            set: this.setNewItems(),
            where: this.conditions.simpleWhere("new")
        });

        const body = buildOneRowBody({
            onInsert: {
                needUpdate: this.hasEffect("new"),
                update: updateNew
            },
            onUpdate: {
                needUpdate: this.needUpdateOnUpdate(),
                noChanges: this.conditions.noChanges(),
                update: updateNew
            },
            onDelete: {
                needUpdate: this.hasEffect("old"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setNulls(),
                    where: this.conditions.simpleWhere("old")
                })
            }
        });
        return body;
    }

    private needUpdateOnUpdate() {
        if ( !this.context.referenceMeta.filters.length ) {
            return;
        }

        const matchedOld: CoalesceFalseExpression[] = [];
        const matchedNew: CoalesceFalseExpression[] = [];
        this.context.referenceMeta.filters.forEach(filter => {
            const filterOld = this.replaceTriggerTableToRow("old", filter);
            const filterNew = this.replaceTriggerTableToRow("new", filter);

            matchedNew.push(
                new CoalesceFalseExpression(filterNew)
            );
            matchedOld.push(
                new CoalesceFalseExpression(filterOld)
            );
        });

        return Expression.or([
            Expression.and(matchedOld),
            Expression.and(matchedNew)
        ]);
    }

    private setNewItems() {
        const setItems = this.context.cache.select.columns.map(selectColumn => {
            const setItem = new SetItem({
                column: selectColumn.name,
                value: this.replaceTriggerTableToRow(
                    "new", selectColumn.expression
                )
            })
            return setItem;
        });
        return setItems;
    }

    private setNulls() {
        const setItems = this.context.cache.select.columns.map(selectColumn => {
            const setItem = new SetItem({
                column: selectColumn.name,
                value: Expression.and(["null"])
            })
            return setItem;
        });
        return setItems;
    }

    private hasEffect(row: Row) {
        const conditions = this.context.cache.select.columns.map(selectColumn => {
            const expression = this.replaceTriggerTableToRow(
                row, selectColumn.expression
            );
            return expression.toString() + " is not null";
        });

        if ( this.context.referenceMeta.filters.length ) {
            const filters = this.context.referenceMeta.filters.map(filter =>
                this.replaceTriggerTableToRow(row, filter)
            );
            const hasEffectAndFilters = Expression.and([
                Expression.or(conditions),
                Expression.and(filters)
            ]);
            return hasEffectAndFilters;
        }

        return Expression.or(conditions);
    }
}