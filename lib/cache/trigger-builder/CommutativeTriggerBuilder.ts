import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildCommutativeBody } from "./body/buildCommutativeBody";
import { Update } from "../../ast";

export class CommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const deltaUpdate = new Update({
            table: this.context.cache.for.toString(),
            set: this.deltaSetItemsFactory.delta(),
            where: this.conditionBuilder.getSimpleWhere("new")
        });

        const body = buildCommutativeBody(
            this.conditionBuilder.hasMutableColumns(),
            this.conditionBuilder.getNoChanges(),
            {
                needUpdate: this.conditionBuilder.getNeedUpdateCondition("old"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItemsFactory.minus(),
                    where: this.conditionBuilder.getSimpleWhere("old")
                })
            },
            {
                needUpdate: this.conditionBuilder.getNeedUpdateCondition("new"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItemsFactory.plus(),
                    where: this.conditionBuilder.getSimpleWhere("new")
                })
            },
            deltaUpdate.set.length > 0 ? {
                needUpdate: this.conditionBuilder.getNoReferenceChanges(),
                update: deltaUpdate,
                old: {
                    needUpdate: this.conditionBuilder.getNeedUpdateConditionOnUpdate("old"),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: this.setItemsFactory.minus(),
                        where: this.conditionBuilder.getSimpleWhereOnUpdate("old")
                    })
                },
                new: {
                    needUpdate: this.conditionBuilder.getNeedUpdateConditionOnUpdate("new"),
                    update: new Update({
                        table: this.context.cache.for.toString(),
                        set: this.setItemsFactory.plus(),
                        where: this.conditionBuilder.getSimpleWhereOnUpdate("new")
                    })
                }
            } : undefined
        );
        
        return body;
    }
}