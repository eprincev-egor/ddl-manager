import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildCommutativeBody } from "../processor/buildCommutativeBody";
import { buildUpdate } from "../processor/buildUpdate";

export class CommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const conditions = this.conditionBuilder.build();

        const mutableColumns = this.triggerTableColumns
            .filter(col => col !== "id");
        const mutableColumnsDepsInAggregations = mutableColumns
            .filter(col => 
                !this.referenceMeta.columns.includes(col)
            );

        const body = buildCommutativeBody(
            mutableColumns,
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
            mutableColumnsDepsInAggregations.length ? {
                needUpdate: conditions.noReferenceChanges,
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    conditions.whereNew,
                    [],
                    "delta"
                )
            } : undefined
        );
        
        return body;
    }
}