import {
    Expression
} from "../../ast";
import { noReferenceChanges } from "../processor/condition/noReferenceChanges";
import { hasEffect } from "../processor/condition/hasEffect";
import { hasReference } from "../processor/condition/hasReference";
import { buildSimpleWhere } from "../processor/condition/buildSimpleWhere";
import { isNotDistinctFrom } from "../processor/condition/isNotDistinctFrom";

import { buildCommutativeBodyWithJoins } from "../processor/buildCommutativeBodyWithJoins";
import { buildUpdate } from "../processor/buildUpdate";
import { buildJoins } from "../processor/buildJoins";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";

export class JoinedCommutativeTriggerBuilder extends AbstractTriggerBuilder {

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
        const noChanges = isNotDistinctFrom(mutableColumns);

        const oldJoins = buildJoins(joins, "old");
        const newJoins = buildJoins(joins, "new");
        
        const bodyWithJoins = buildCommutativeBodyWithJoins(
            noChanges,
            {
                hasReference: hasReference(
                    this.triggerTable,
                    this.referenceMeta,
                    "old"
                ),
                needUpdate: hasEffect(
                    this.cache,
                    this.triggerTable,
                    "old",
                    joins
                ) as Expression,
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
                hasReference: hasReference(
                    this.triggerTable,
                    this.referenceMeta,
                    "new"
                ),
                needUpdate: hasEffect(
                    this.cache,
                    this.triggerTable,
                    "new",
                    joins
                ) as Expression,
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
                hasReference: hasReference(
                    this.triggerTable,
                    this.referenceMeta,
                    "new"
                ),
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
                ),
                joins: newJoins
            }
        );

        return bodyWithJoins;
    }
}