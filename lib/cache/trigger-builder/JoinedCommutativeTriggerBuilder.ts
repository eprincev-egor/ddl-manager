import {
    Expression
} from "../../ast";
import { buildCommutativeBodyWithJoins } from "../processor/buildCommutativeBodyWithJoins";
import { buildUpdate } from "../processor/buildUpdate";
import { buildJoins } from "../processor/buildJoins";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";

export class JoinedCommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const conditions = this.conditionBuilder.build();
        
        const joins = findJoinsMeta(this.context.cache.select);

        const oldJoins = buildJoins(joins, "old");
        const newJoins = buildJoins(joins, "new");
        
        const bodyWithJoins = buildCommutativeBodyWithJoins(
            conditions.noChanges,
            {
                hasReference: conditions.hasOldReference,
                needUpdate: conditions.hasOldEffect as Expression,
                update: buildUpdate(
                    this.context,
                    conditions.whereOld,
                    joins,
                    "minus"
                ),
                joins: oldJoins
            },
            {
                hasReference: conditions.hasNewReference,
                needUpdate: conditions.hasNewEffect as Expression,
                update: buildUpdate(
                    this.context,
                    conditions.whereNew,
                    joins,
                    "plus"
                ),
                joins: newJoins
            },
            {
                hasReference: conditions.hasNewReference,
                needUpdate: conditions.noReferenceChanges,
                update: buildUpdate(
                    this.context,
                    conditions.whereNew,
                    joins,
                    "delta"
                ),
                joins: newJoins
            }
        );

        return bodyWithJoins;
    }
}