import {
    Expression
} from "../../ast";
import { buildSimpleWhere } from "./condition/buildSimpleWhere";

import { buildCommutativeBodyWithJoins } from "../processor/buildCommutativeBodyWithJoins";
import { buildUpdate } from "../processor/buildUpdate";
import { buildJoins } from "../processor/buildJoins";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { noChanges } from "./condition/noChanges";

export class JoinedCommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const conditions = this.conditionBuilder.build();
        
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

        const oldJoins = buildJoins(joins, "old");
        const newJoins = buildJoins(joins, "new");
        
        const bodyWithJoins = buildCommutativeBodyWithJoins(
            noChangesCondition,
            {
                hasReference: conditions.hasOldReference,
                needUpdate: conditions.hasOldEffect as Expression,
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    whereOld,
                    joins,
                    "minus"
                ),
                joins: oldJoins
            },
            {
                hasReference: conditions.hasNewReference,
                needUpdate: conditions.hasNewEffect as Expression,
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    whereNew,
                    joins,
                    "plus"
                ),
                joins: newJoins
            },
            {
                hasReference: conditions.hasNewReference,
                needUpdate: conditions.noReferenceChanges,
                update: buildUpdate(
                    this.cache,
                    this.triggerTable,
                    whereNew,
                    joins,
                    "delta"
                ),
                joins: newJoins
            }
        );

        return bodyWithJoins;
    }
}