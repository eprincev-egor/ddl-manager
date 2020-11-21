import { noReferenceChanges } from "./condition/noReferenceChanges";
import { buildCommutativeBody } from "../processor/buildCommutativeBody";
import { buildNeedUpdateCondition } from "./condition/buildNeedUpdateCondition";
import { buildSimpleWhere } from "./condition/buildSimpleWhere";

import { buildUpdate } from "../processor/buildUpdate";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { noChanges } from "./condition/noChanges";


export class CommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const mutableColumns = this.triggerTableColumns
            .filter(col => col !== "id");
        
        const joins = findJoinsMeta(this.cache.select);

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
        const noChangesCondition = noChanges(
            this.triggerTableColumns,
            this.triggerTable,
            this.databaseStructure
        );

        const mutableColumnsDepsInAggregations = mutableColumns
            .filter(col => 
                !this.referenceMeta.columns.includes(col)
            );

        const body = buildCommutativeBody(
            mutableColumns,
            noChangesCondition,
            {
                needUpdate: buildNeedUpdateCondition(
                    this.cache,
                    this.triggerTable,
                    this.referenceMeta,
                    "old"
                ),
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    whereOld,
                    joins,
                    "minus"
                )
            },
            {
                needUpdate: buildNeedUpdateCondition(
                    this.cache,
                    this.triggerTable,
                    this.referenceMeta,
                    "new"
                ),
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    whereNew,
                    joins,
                    "plus"
                )
            },
            mutableColumnsDepsInAggregations.length ? {
                needUpdate: noReferenceChanges(
                    this.referenceMeta,
                    this.triggerTable,
                    this.databaseStructure
                ),
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    whereNew,
                    joins,
                    "delta"
                )
            } : undefined
        );
        
        return body;
    }
}