import { AbstractTriggerBuilder } from "../AbstractTriggerBuilder";
import { buildArrayCommutativeBody, IArrVar } from "../body/buildArrayCommutativeBody";
import { ColumnReference, Expression, Update } from "../../../ast";
import { buildArrVars } from "../../processor/buildArrVars";
import { CoalesceFalseExpression } from "../../../ast/expression/CoalesceFalseExpression";
import { TableReference } from "../../../database/schema/TableReference";
import { findJoinsMeta } from "../../processor/findJoinsMeta";
import { buildJoinVariables } from "../../processor/buildJoinVariables";
import { hasReference } from "../condition/hasReference";

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
                    needUpdate: this.hasReferenceWithArrVars(
                        deletedArrElements
                    ),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: this.setItems.minus(),
                        where: this.conditions.simpleWhereOnUpdate("old", "deleted_")
                    })
                },
                notChanged: deltaUpdate.set.length ? {
                    needUpdate: this.hasReferenceWithArrVars(
                        notChangedArrElements
                    ),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: this.deltaSetItems.delta(),
                        where: this.conditions.simpleWhereOnUpdate("new", "not_changed_")
                    })
                } : undefined,
                inserted: {
                    needUpdate: this.hasReferenceWithArrVars(
                        insertedArrElements
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
            )
            .replaceTable(
                this.context.triggerTable,
                new TableReference(this.context.triggerTable, row)
            )
        );
        return this.conditions.replaceTriggerTableRefsTo(matchedExpression, row)!;
    }

    private buildJoins(row: "new" | "old") {
        const joins = findJoinsMeta(this.context.cache.select);
        return buildJoinVariables(this.context.database, joins, row);
    }

    private hasReferenceWithArrVars(arrVars: IArrVar[]) {
        let refCondition = this.conditions.replaceTriggerTableRefsTo(
            hasReference(this.context)!, "new"
        )!;

        const tableRef = new TableReference(
            this.context.triggerTable,
            "new"
        );
        for (const arrVar of arrVars) {
            const columnRef = new ColumnReference(
                tableRef,
                arrVar.triggerColumn
            );
            refCondition = refCondition.replaceColumn(
                columnRef,
                Expression.unknown(arrVar.name)
            );
        }

        return refCondition;
    }
}