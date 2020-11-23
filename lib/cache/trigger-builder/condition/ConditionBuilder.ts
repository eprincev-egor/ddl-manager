import { noReferenceChanges } from "./noReferenceChanges";
import { buildNeedUpdateCondition } from "./buildNeedUpdateCondition";
import { noChanges } from "./noChanges";
import { hasReference } from "./hasReference";
import { hasEffect } from "./hasEffect";
import { findJoinsMeta } from "../../processor/findJoinsMeta";
import { buildSimpleWhere } from "./buildSimpleWhere";
import { replaceArrayNotNullOn } from "./replaceArrayNotNullOn";
import { CacheContext } from "../CacheContext";

export class ConditionBuilder {
    private readonly context: CacheContext;
    constructor(
        context: CacheContext
    ) {
        this.context = context;
    }

    build() {
        const joins = findJoinsMeta(this.context.cache.select);

        const mutableColumns = this.context.triggerTableColumns
            .filter(col => col !== "id");
        const mutableColumnsDepsInAggregations = mutableColumns
            .filter(col => 
                !this.context.referenceMeta.columns.includes(col)
            );

        const conditions = {

            hasMutableColumns: 
                mutableColumns.length > 0,
            hasMutableColumnsDepsInAggregations: 
                mutableColumnsDepsInAggregations.length > 0,
            
            noReferenceChanges: noReferenceChanges(
                this.context
            ),
            noChanges: noChanges(
                this.context
            ),
            needUpdateOnInsert: buildNeedUpdateCondition(
                this.context,
                "new"
            ),
            needUpdateOnDelete: buildNeedUpdateCondition(
                this.context,
                "old"
            ),
            needUpdateOnUpdateOld: replaceArrayNotNullOn(
                this.context,
                buildNeedUpdateCondition(
                    this.context,
                    "old"
                ),
                "cm_get_deleted_elements"
            ),
            needUpdateOnUpdateNew: replaceArrayNotNullOn(
                this.context,
                buildNeedUpdateCondition(
                    this.context,
                    "new"
                ),
                "cm_get_inserted_elements"
            ),
            hasOldReference: hasReference(
                this.context,
                "old"
            ),
            hasNewReference: hasReference(
                this.context,
                "new"
            ),
            hasOldEffect: hasEffect(
                this.context,
                "old",
                joins
            ),
            hasNewEffect: hasEffect(
                this.context,
                "new",
                joins
            ),
            whereOld: buildSimpleWhere(
                this.context,
                "old"
            ),
            whereNew: buildSimpleWhere(
                this.context,
                "new"
            ),
            whereOldOnUpdate: replaceArrayNotNullOn(
                this.context,
                buildSimpleWhere(
                    this.context,
                    "old"
                ),
                "cm_get_deleted_elements"
            ),
            whereNewOnUpdate: replaceArrayNotNullOn(
                this.context,
                buildSimpleWhere(
                    this.context,
                    "new"
                ),
                "cm_get_inserted_elements"
            )
        };
        return conditions;
    }
}
