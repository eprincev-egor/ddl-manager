import { Update, Expression, SetItem } from "../../ast";
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

        return Expression.or(conditions);
    }

    private replaceTriggerTableToRow(row: Row, expression: Expression) {
        return expression.replaceTable(
            this.context.triggerTable,
            new TableReference(
                this.context.triggerTable,
                row
            )
        );
    }
}