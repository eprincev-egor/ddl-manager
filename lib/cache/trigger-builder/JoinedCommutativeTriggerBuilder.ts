import { Update } from "../../ast";
import { buildCommutativeBodyWithJoins } from "./body/buildCommutativeBodyWithJoins";
import { buildJoins } from "../processor/buildJoins";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";

export class JoinedCommutativeTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const bodyWithJoins = buildCommutativeBodyWithJoins(
            this.conditions.noChanges(),
            {
                hasReference: this.conditions.hasReference("old"),
                needUpdate: this.conditions.mathOneFilter("old"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItems.minus(),
                    where: this.conditions.simpleWhere("old")
                }),
                joins: this.buildJoins("old")
            },
            {
                hasReference: this.conditions.hasReference("new"),
                needUpdate: this.conditions.mathOneFilter("new"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setItems.plus(),
                    where: this.conditions.simpleWhere("new")
                }),
                joins: this.buildJoins("new")
            },
            {
                hasReference: this.conditions.hasReference("new"),
                needUpdate: this.conditions.noReferenceChanges(),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.deltaSetItems.delta(),
                    where: this.conditions.simpleWhere("new")
                }),
                joins: this.buildJoins("new")
            }
        );
        return bodyWithJoins;
    }

    private buildJoins(row: "new" | "old") {
        const joins = findJoinsMeta(this.context.cache.select);
        return buildJoins(this.context.database, joins, row);
    }
}