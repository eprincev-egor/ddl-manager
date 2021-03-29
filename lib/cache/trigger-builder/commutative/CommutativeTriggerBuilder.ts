import { AbstractTriggerBuilder } from "../AbstractTriggerBuilder";
import { buildCommutativeBody } from "../body/buildCommutativeBody";
import { Update } from "../../../ast";
import { buildJoinVariables } from "../../processor/buildJoinVariables";
import { findJoinsMeta } from "../../processor/findJoinsMeta";

export class CommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const deltaUpdate = new Update({
            table: this.context.cache.for.toString(),
            set: this.deltaSetItems.delta(),
            where: this.conditions.simpleWhere("new")
        });

        const body = buildCommutativeBody(
            this.context.withoutInsertCase() ? false : true,
            this.conditions.hasMutableColumns(),
            this.conditions.noChanges(),
            this.buildJoins("old"),
            this.buildJoins("new"),
            {
                hasReferenceWithoutJoins: this.conditions.hasReferenceWithoutJoins("old"),
                needUpdate: this.conditions.filtersWithJoins("old"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItems.minus(),
                    where: this.conditions.simpleWhere("old")
                })
            },
            {
                hasReferenceWithoutJoins: this.conditions.hasReferenceWithoutJoins("new"),
                needUpdate: this.conditions.filtersWithJoins("new"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItems.plus(),
                    where: this.conditions.simpleWhere("new")
                })
            },
            {
                needUpdate: this.conditions.noReferenceChanges(),
                update: deltaUpdate,
                exitIf: this.conditions.exitFromDeltaUpdateIf(),
                old: {
                    needUpdate: this.conditions.needUpdateConditionOnUpdate("old"),
                    update: this.needUpdateInDelta() ? new Update({
                        table: this.context.cache.for.toString(),
                        set: this.setItems.minus(),
                        where: this.conditions.simpleWhereOnUpdate(
                            "old",
                            "deleted_"
                        )
                    }) : undefined
                },
                new: {
                    needUpdate: this.conditions.needUpdateConditionOnUpdate("new"),
                    update: this.needUpdateInDelta() ? new Update({
                        table: this.context.cache.for.toString(),
                        set: this.setItems.plus(),
                        where: this.conditions.simpleWhereOnUpdate("new", "inserted_")
                    }) : undefined
                }
            }
        );
        
        return body;
    }

    private needUpdateInDelta() {
        const hasCondition = !!this.conditions.needUpdateConditionOnUpdate("new");
        const hasUnknownReferenceExpressions = this.context.referenceMeta.unknownExpressions.length;

        return (
            hasCondition ||
            hasUnknownReferenceExpressions
        );
    }

    private buildJoins(row: "new" | "old") {
        const joins = findJoinsMeta(this.context.cache.select);
        return buildJoinVariables(this.context.database, joins, row);
    }
}