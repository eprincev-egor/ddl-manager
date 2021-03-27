import { AbstractTriggerBuilder } from "../AbstractTriggerBuilder";
import { buildArrayCommutativeBody } from "../body/buildArrayCommutativeBody";
import { Expression, Update } from "../../../ast";
import { buildArrVars } from "../../processor/buildArrVars";
import { CoalesceFalseExpression } from "../../../ast/expression/CoalesceFalseExpression";
import { TableReference } from "../../../database/schema/TableReference";

export class ArrayRefCommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const insertedArrElements = buildArrVars(this.context, "inserted_");
        const notChangedArrElements = buildArrVars(this.context, "not_changed_");
        const deletedArrElements = buildArrVars(this.context, "deleted_");

        const body = buildArrayCommutativeBody({
            needInsertCase: this.context.withoutInsertCase() ? false : true,
            hasMutableColumns: this.conditions.hasMutableColumns(),
            noChanges: this.conditions.noChanges(),
            insertCase: {
                hasReference: this.conditions.hasReferenceWithoutJoins("old"),
                needUpdate: this.conditions.filtersWithJoins("old"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItems.minus(),
                    where: this.conditions.simpleWhere("old")
                })
            },
            deleteCase: {
                hasReference: this.conditions.hasReferenceWithoutJoins("new"),
                needUpdate: this.conditions.filtersWithJoins("new"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItems.plus(),
                    where: this.conditions.simpleWhere("new")
                })
            },
            updateCase: {
                deleted: {
                    needUpdate: Expression.and(
                        deletedArrElements.map(deletedVar =>
                            `${deletedVar.name} is not null`
                        )
                    ),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: this.setItems.minus(),
                        where: this.conditions.simpleWhereOnUpdate("old", "deleted_")
                    })
                },
                notChanged: {
                    needUpdate: Expression.and(
                        notChangedArrElements.map(notChangedVar =>
                            `${notChangedVar.name} is not null`
                        )
                    ),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: this.deltaSetItems.delta(),
                        where: this.conditions.simpleWhereOnUpdate("new", "not_changed_")
                    })
                },
                inserted: {
                    needUpdate: Expression.and(
                        insertedArrElements.map(insertedVar =>
                            `${insertedVar.name} is not null`
                        )
                    ),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: this.setItems.plus(),
                        where: this.conditions.simpleWhereOnUpdate("new", "inserted_")
                    })
                }
            },
            insertedArrElements,
            notChangedArrElements,
            deletedArrElements,
            matchedNew: this.matched("new"),
            matchedOld: this.matched("old")
        });
        
        return body;
    }

    private matched(row: string) {
        const matchedExpression = new CoalesceFalseExpression(
            Expression.and(
                this.context.referenceMeta.filters
            ).replaceTable(
                this.context.triggerTable,
                new TableReference(this.context.triggerTable, row)
            )
        );
        return matchedExpression;
    }

}