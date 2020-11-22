import {
    Table,
    Cache,
    Expression
} from "../../../ast";
import { noReferenceChanges } from "./noReferenceChanges";
import { buildNeedUpdateCondition } from "./buildNeedUpdateCondition";
import { buildReferenceMeta } from "./buildReferenceMeta";
import { noChanges } from "./noChanges";
import { hasReference } from "./hasReference";
import { hasEffect } from "./hasEffect";
import { findJoinsMeta } from "../../processor/findJoinsMeta";
import { Database as DatabaseStructure } from "../../schema/Database";
import { buildSimpleWhere } from "./buildSimpleWhere";

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

        const mutableColumns = this.triggerTableColumns
            .filter(col => col !== "id");
        const mutableColumnsDepsInAggregations = mutableColumns
            .filter(col => 
                !referenceMeta.columns.includes(col)
            );

        const conditions = {

            hasMutableColumns: 
                mutableColumns.length > 0,
            hasMutableColumnsDepsInAggregations: 
                mutableColumnsDepsInAggregations.length > 0,
            
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
            needUpdateOnUpdateOld: replaceArrayNotNullOn(
                buildNeedUpdateCondition(
                    this.cache,
                    this.triggerTable,
                    referenceMeta,
                    "old"
                ),
                this.triggerTable,
                this.databaseStructure,
                "cm_get_deleted_elements"
            ),
            needUpdateOnUpdateNew: replaceArrayNotNullOn(
                buildNeedUpdateCondition(
                    this.cache,
                    this.triggerTable,
                    referenceMeta,
                    "new"
                ),
                this.triggerTable,
                this.databaseStructure,
                "cm_get_inserted_elements"
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
            ),
            whereOld: buildSimpleWhere(
                this.cache,
                this.triggerTable,
                "old",
                referenceMeta
            ),
            whereNew: buildSimpleWhere(
                this.cache,
                this.triggerTable,
                "new",
                referenceMeta
            ),
            whereOldOnUpdate: replaceArrayNotNullOn(
                buildSimpleWhere(
                    this.cache,
                    this.triggerTable,
                    "old",
                    referenceMeta
                ),
                this.triggerTable,
                this.databaseStructure,
                "cm_get_deleted_elements"
            ),
            whereNewOnUpdate: replaceArrayNotNullOn(
                buildSimpleWhere(
                    this.cache,
                    this.triggerTable,
                    "new",
                    referenceMeta
                ),
                this.triggerTable,
                this.databaseStructure,
                "cm_get_inserted_elements"
            )
        };
        return conditions;
    }
}

function replaceArrayNotNullOn(
    sourceExpression: Expression | undefined,
    triggerTable: Table,
    databaseStructure: DatabaseStructure,
    funcName: string
): Expression | undefined {
    if ( !sourceExpression ) {
        return;
    }

    let outputExpression = sourceExpression;
    const tableStructure = databaseStructure.getTable(triggerTable);

    for (const columnRef of sourceExpression.getColumnReferences()) {
        if ( !columnRef.tableReference.table.equal(triggerTable) ) {
            continue;
        }

        const column = tableStructure && tableStructure.getColumn(columnRef.name);
        if ( column && column.type.isArray() ) {
            outputExpression = outputExpression.replaceColumn(
                columnRef.toString(),
                `${funcName}(old.${column.name}, new.${column.name})`
            );
        }
    }

    return outputExpression;
}