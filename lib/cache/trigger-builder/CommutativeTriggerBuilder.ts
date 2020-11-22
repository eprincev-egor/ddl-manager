import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildCommutativeBody } from "../processor/buildCommutativeBody";
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
                    this.cache,
                    this.triggerTable,
                    conditions.whereOld,
                    [],
                    "minus"
                )
            },
            {
                needUpdate: conditions.needUpdateOnInsert,
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    conditions.whereNew,
                    [],
                    "plus"
                )
            },
            conditions.hasMutableColumnsDepsInAggregations ? {
                needUpdate: conditions.noReferenceChanges,
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    conditions.whereNew,
                    [],
                    "delta"
                ),
                old: {
                    needUpdate: conditions.needUpdateOnUpdateOld,
                    update: buildUpdate(
                        this.cache,
                        this.triggerTable,
                        conditions.whereOldOnUpdate,
                        [],
                        "minus"
                    )
                },
                new: {
                    needUpdate: conditions.needUpdateOnUpdateNew,
                    update: buildUpdate(
                        this.cache,
                        this.triggerTable,
                        conditions.whereNewOnUpdate,
                        [],
                        "plus"
                    )
                }
            } : undefined
        );
        
        return body;
    }
}