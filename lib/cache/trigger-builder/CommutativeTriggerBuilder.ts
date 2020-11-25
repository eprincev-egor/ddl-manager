import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildCommutativeBody } from "./body/buildCommutativeBody";
import { buildUpdate } from "../processor/buildUpdate";

export class CommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const body = buildCommutativeBody(
            this.conditionBuilder.hasMutableColumns(),
            this.conditionBuilder.getNoChanges(),
            {
                needUpdate: this.conditionBuilder.getNeedUpdateCondition("old"),
                update: buildUpdate(
                    this.context,
                    this.conditionBuilder.getSimpleWhere("old"),
                    [],
                    "minus"
                )
            },
            {
                needUpdate: this.conditionBuilder.getNeedUpdateCondition("new"),
                update: buildUpdate(
                    this.context,
                    this.conditionBuilder.getSimpleWhere("new"),
                    [],
                    "plus"
                )
            },
            this.conditionBuilder.hasMutableColumnsDepsInAggregations() ? {
                needUpdate: this.conditionBuilder.getNoReferenceChanges(),
                update: buildUpdate(
                    this.context,
                    this.conditionBuilder.getSimpleWhere("new"),
                    [],
                    "delta"
                ),
                old: {
                    needUpdate: this.conditionBuilder.getNeedUpdateConditionOnUpdate("old"),
                    update: buildUpdate(
                        this.context,
                        this.conditionBuilder.getSimpleWhereOnUpdate("old"),
                        [],
                        "minus"
                    )
                },
                new: {
                    needUpdate: this.conditionBuilder.getNeedUpdateConditionOnUpdate("new"),
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