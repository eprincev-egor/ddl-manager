import {
    Expression
} from "../../ast";
import { buildCommutativeBodyWithJoins } from "./body/buildCommutativeBodyWithJoins";
import { buildUpdate } from "../processor/buildUpdate";
import { buildJoins } from "../processor/buildJoins";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";

export class JoinedCommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const joins = findJoinsMeta(this.context.cache.select);

        const oldJoins = buildJoins(joins, "old");
        const newJoins = buildJoins(joins, "new");
        
        const bodyWithJoins = buildCommutativeBodyWithJoins(
            this.conditionBuilder.getNoChanges(),
            {
                hasReference: this.conditionBuilder.getHasReference("old"),
                needUpdate: this.conditionBuilder.getHasEffect("old") as Expression,
                update: buildUpdate(
                    this.context,
                    this.conditionBuilder.getSimpleWhere("old"),
                    joins,
                    "minus"
                ),
                joins: oldJoins
            },
            {
                hasReference: this.conditionBuilder.getHasReference("new"),
                needUpdate: this.conditionBuilder.getHasEffect("new") as Expression,
                update: buildUpdate(
                    this.context,
                    this.conditionBuilder.getSimpleWhere("new"),
                    joins,
                    "plus"
                ),
                joins: newJoins
            },
            {
                hasReference: this.conditionBuilder.getHasReference("new"),
                needUpdate: this.conditionBuilder.getNoReferenceChanges(),
                update: buildUpdate(
                    this.context,
                    this.conditionBuilder.getSimpleWhere("new"),
                    joins,
                    "delta"
                ),
                joins: newJoins
            }
        );

        return bodyWithJoins;
    }
}