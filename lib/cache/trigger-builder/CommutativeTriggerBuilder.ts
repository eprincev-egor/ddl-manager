import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildCommutativeBody } from "./body/buildCommutativeBody";
import { buildUpdate } from "../processor/buildUpdate";

export class CommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const conditions = this.conditionBuilder.build();

        const body = buildCommutativeBody(
            conditions.hasMutableColumns,
            conditions.noChanges,
            {
                needUpdate: conditions.needUpdateOnDelete,
                update: buildUpdate(
                    this.context,
                    this.conditionBuilder.getSimpleWhere("old"),
                    [],
                    "minus"
                )
            },
            {
                needUpdate: conditions.needUpdateOnInsert,
                update: buildUpdate(
                    this.context,
                    this.conditionBuilder.getSimpleWhere("new"),
                    [],
                    "plus"
                )
            },
            conditions.hasMutableColumnsDepsInAggregations ? {
                needUpdate: conditions.noReferenceChanges,
                update: buildUpdate(
                    this.context,
                    this.conditionBuilder.getSimpleWhere("new"),
                    [],
                    "delta"
                ),
                old: {
                    needUpdate: conditions.needUpdateOnUpdateOld,
                    update: buildUpdate(
                        this.context,
                        this.conditionBuilder.getSimpleWhereOnUpdate("old"),
                        [],
                        "minus"
                    )
                },
                new: {
                    needUpdate: conditions.needUpdateOnUpdateNew,
                    update: buildUpdate(
                        this.context,
                        this.conditionBuilder.getSimpleWhereOnUpdate("new"),
                        [],
                        "plus"
                    )
                }
            } : undefined
        );
        
        return body;
    }
}