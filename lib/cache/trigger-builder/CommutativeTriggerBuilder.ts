import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildCommutativeBody } from "./body/buildCommutativeBody";
import { Update } from "../../ast";

export class CommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const deltaUpdate = new Update({
            table: this.context.cache.for.toString(),
            set: this.deltaSetItems.delta(),
            where: this.conditions.simpleWhere("new")
        });

        const body = buildCommutativeBody(
            this.conditions.hasMutableColumns(),
            this.conditions.noChanges(),
            {
                needUpdate: this.conditions.needUpdateCondition("old"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItems.minus(),
                    where: this.conditions.simpleWhere("old")
                })
            },
            {
                needUpdate: this.conditions.needUpdateCondition("new"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItems.plus(),
                    where: this.conditions.simpleWhere("new")
                })
            },
            deltaUpdate.set.length > 0 ? {
                needUpdate: this.conditions.noReferenceChanges(),
                update: deltaUpdate,
                old: {
                    needUpdate: this.conditions.needUpdateConditionOnUpdate("old"),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: this.setItems.minus(),
                        where: this.conditions.simpleWhereOnUpdate("old")
                    })
                },
                new: {
                    needUpdate: this.conditions.needUpdateConditionOnUpdate("new"),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: this.setItems.plus(),
                        where: this.conditions.simpleWhereOnUpdate("new")
                    })
                }
            } : undefined
        );
        
        return body;
    }
}