import { AbstractTriggerBuilder } from "../AbstractTriggerBuilder";
import { buildArrayCommutativeBody } from "../body/buildArrayCommutativeBody";
import { ColumnReference, Expression, Update } from "../../../ast";
import { buildArrVars, IArrVar } from "../../processor/buildArrVars";
import { CoalesceFalseExpression } from "../../../ast/expression/CoalesceFalseExpression";
import { TableReference } from "../../../database/schema/TableReference";
import { findJoinsMeta } from "../../processor/findJoinsMeta";
import { buildJoinVariables } from "../../processor/buildJoinVariables";
import { hasReference } from "../condition/hasReference";

export class ArrayRefCommutativeTriggerBuilder
extends AbstractTriggerBuilder {

    createTriggers() {
        return [{
            trigger: this.createDatabaseTriggerOnDIU(),
            procedure: this.createDatabaseFunction(
                this.createBody()
            )
        }];
    }

    protected createBody() {
        const deltaSetItems = this.deltaSetItems.delta();

        const insertedArrElements = buildArrVars(this.context, "inserted_");
        const notChangedArrElements = deltaSetItems.length ?
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
                        where: this.simpleWhereOnUpdate(
                            "old", deletedArrElements
                        )
                    })
                },
                notChanged: deltaSetItems.length ? {
                    needUpdate: this.hasReferenceWithArrVars(
                        notChangedArrElements
                    ),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: deltaSetItems,
                        where: this.simpleWhereOnUpdate(
                            "new", notChangedArrElements
                        )
                    })
                } : undefined,
                inserted: {
                    needUpdate: this.hasReferenceWithArrVars(
                        insertedArrElements
                    ),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: this.setItems.plus(),
                        where: this.simpleWhereOnUpdate(
                            "new", insertedArrElements
                        )
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
        const refCondition = this.conditions.replaceTriggerTableRefsTo(
            hasReference(this.context)!, "new"
        )!;
        return this.replaceArrayColumnsToVariables(
            refCondition,
            arrVars,
            "new"
        );
    }

    private simpleWhereOnUpdate(row: string, arrVars: IArrVar[]) {
        const where = this.conditions.simpleWhereOnUpdate(row)!;

        return this.replaceArrayColumnsToVariables(
            where, arrVars, row
        );
    }

    private replaceArrayColumnsToVariables(
        expression: Expression,
        arrVars: IArrVar[],
        row: string
    ) {
        const tableRef = new TableReference(
            this.context.triggerTable,
            row
        );
        for (const arrVar of arrVars) {
            const columnRef = new ColumnReference(
                tableRef,
                arrVar.triggerColumn
            );
            expression = expression.replaceColumn(
                columnRef,
                Expression.unknown(arrVar.name)
            );
        }

        return expression;
    }
}