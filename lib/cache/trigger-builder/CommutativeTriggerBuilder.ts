import { buildCommutativeBody } from "../processor/buildCommutativeBody";
import { buildSimpleWhere } from "./condition/buildSimpleWhere";
import { buildUpdate } from "../processor/buildUpdate";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";

export class CommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const conditions = this.conditionBuilder.build();

        const whereOld = buildSimpleWhere(
            this.cache,
            this.triggerTable,
            "old",
            this.referenceMeta
        );
        const whereNew = buildSimpleWhere(
            this.cache,
            this.triggerTable,
            "new",
            this.referenceMeta
        );

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
                    whereOld,
                    [],
                    "minus"
                )
            },
            {
                needUpdate: conditions.needUpdateOnInsert,
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    whereNew,
                    [],
                    "plus"
                )
            },
            mutableColumnsDepsInAggregations.length ? {
                needUpdate: conditions.noReferenceChanges,
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    whereNew,
                    [],
                    "delta"
                )
            } : undefined
        );
        
        return body;
    }
}