import {
    Table,
    Cache
} from "../../../ast";
import { noReferenceChanges } from "./noReferenceChanges";
import { buildNeedUpdateCondition } from "./buildNeedUpdateCondition";
import { buildReferenceMeta } from "./buildReferenceMeta";
import { noChanges } from "./noChanges";
import { hasReference } from "./hasReference";
import { hasEffect } from "./hasEffect";
import { findJoinsMeta } from "../../processor/findJoinsMeta";
import { Database as DatabaseStructure } from "../../schema/Database";

export class ConditionBuilder {
    private readonly cache: Cache;
    private readonly triggerTable: Table;
    private readonly triggerTableColumns: string[];
    private readonly databaseStructure: DatabaseStructure;
    constructor(
        cache: Cache,
        triggerTable: Table,
        triggerTableColumns: string[],
        databaseStructure: DatabaseStructure
    ) {
        this.cache = cache;
        this.triggerTable = triggerTable;
        this.triggerTableColumns = triggerTableColumns;
        this.databaseStructure = databaseStructure;
    }

    build() {
        const joins = findJoinsMeta(this.cache.select);
        const referenceMeta = buildReferenceMeta(
            this.cache,
            this.triggerTable
        );

        return {
            noReferenceChanges: noReferenceChanges(
                referenceMeta,
                this.triggerTable,
                this.databaseStructure
            ),
            noChanges: noChanges(
                this.triggerTableColumns,
                this.triggerTable,
                this.databaseStructure
            ),
            needUpdateOnInsert: buildNeedUpdateCondition(
                this.cache,
                this.triggerTable,
                referenceMeta,
                "new"
            ),
            needUpdateOnDelete: buildNeedUpdateCondition(
                this.cache,
                this.triggerTable,
                referenceMeta,
                "old"
            ),
            hasOldReference: hasReference(
                this.triggerTable,
                referenceMeta,
                "old"
            ),
            hasNewReference: hasReference(
                this.triggerTable,
                referenceMeta,
                "new"
            ),
            hasOldEffect: hasEffect(
                this.cache,
                this.triggerTable,
                "old",
                joins
            ),
            hasNewEffect: hasEffect(
                this.cache,
                this.triggerTable,
                "new",
                joins
            )
        };
    }
}