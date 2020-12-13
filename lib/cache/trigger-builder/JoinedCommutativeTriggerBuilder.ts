import {
    Expression, Update
} from "../../ast";
import { buildCommutativeBodyWithJoins } from "./body/buildCommutativeBodyWithJoins";
import { buildUpdate } from "../processor/buildUpdate";
import { buildJoins } from "../processor/buildJoins";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";

export class JoinedCommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const joins = findJoinsMeta(this.context.cache.select);

        const bodyWithJoins = buildCommutativeBodyWithJoins(
            this.conditionBuilder.getNoChanges(),
            {
                hasReference: this.conditionBuilder.getHasReference("old"),
                needUpdate: this.conditionBuilder.getHasEffect("old") as Expression,
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItemsFactory.minus(),
                    where: this.conditionBuilder.getSimpleWhere("old")
                }),
                joins: this.buildJoins("old")
            },
            {
                hasReference: this.conditionBuilder.getHasReference("new"),
                needUpdate: this.conditionBuilder.getHasEffect("new") as Expression,
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItemsFactory.plus(),
                    where: this.conditionBuilder.getSimpleWhere("new")
                }),
                joins: this.buildJoins("new")
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
                joins: this.buildJoins("new")
            }
        );

        return bodyWithJoins;
    }

    private buildJoins(row: "new" | "old") {
        const joins = findJoinsMeta(this.context.cache.select);
        return buildJoins(joins, row);
    }
}