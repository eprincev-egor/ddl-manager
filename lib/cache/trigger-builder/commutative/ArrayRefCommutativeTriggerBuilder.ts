import { AbstractTriggerBuilder } from "../AbstractTriggerBuilder";
import { buildArrayCommutativeBody } from "../body/buildArrayCommutativeBody";
import { Expression, Update } from "../../../ast";
import { buildArrVars } from "../../processor/buildArrVars";
import { CoalesceFalseExpression } from "../../../ast/expression/CoalesceFalseExpression";
import { TableReference } from "../../../database/schema/TableReference";
import { findJoinsMeta } from "../../processor/findJoinsMeta";
import { buildJoinVariables } from "../../processor/buildJoinVariables";

export class ArrayRefCommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const deltaUpdate = new Update({
            table: this.context.cache.for.toString(),
            set: this.deltaSetItems.delta(),
            where: this.conditions.simpleWhereOnUpdate("new", "not_changed_")
        });

        const insertedArrElements = buildArrVars(this.context, "inserted_");
        const notChangedArrElements = deltaUpdate.set.length ?
            buildArrVars(this.context, "not_changed_") :
            [];
        const deletedArrElements = buildArrVars(this.context, "deleted_");

        const body = buildArrayCommutativeBody({
            needInsertCase: this.context.withoutInsertCase() ? false : true,
            hasMutableColumns: this.conditions.hasMutableColumns(),
            noChanges: this.conditions.noChanges(),
            deleteCase: {
                hasReference: this.conditions.hasReferenceWithoutJoins("old"),
                needUpdate: this.conditions.filtersWithJoins("old"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItems.minus(),
                    where: this.conditions.simpleWhere("old")
                })
            },
            insertCase: {
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
                notChanged: deltaUpdate.set.length ? {
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
                } : undefined,
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
            needMatching: this.context.referenceMeta.filters.length > 0,
            matchedNew: this.matched("new"),
            matchedOld: this.matched("old"),
            oldJoins: this.buildJoins("old"),
            newJoins: this.buildJoins("new")
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

    private buildJoins(row: "new" | "old") {
        const joins = findJoinsMeta(this.context.cache.select);
        return buildJoinVariables(this.context.database, joins, row);
    }
}