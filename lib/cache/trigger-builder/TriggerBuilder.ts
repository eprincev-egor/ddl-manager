import {
    Expression,
    Table,
    Cache,
    DatabaseFunction,
    DatabaseTrigger
} from "../../ast";
import { buildReferenceMeta, IReferenceMeta } from "../processor/condition/buildReferenceMeta";
import { noReferenceChanges } from "../processor/condition/noReferenceChanges";
import { buildCommutativeBody } from "../processor/buildCommutativeBody";
import { buildNeedUpdateCondition } from "../processor/condition/buildNeedUpdateCondition";
import { hasEffect } from "../processor/condition/hasEffect";
import { hasReference } from "../processor/condition/hasReference";
import { buildSimpleWhere } from "../processor/condition/buildSimpleWhere";
import { isNotDistinctFrom } from "../processor/condition/isNotDistinctFrom";

import { buildCommutativeBodyWithJoins } from "../processor/buildCommutativeBodyWithJoins";
import { buildUpdate } from "../processor/buildUpdate";
import { buildJoins } from "../processor/buildJoins";
import { buildUniversalBody } from "../processor/buildUniversalBody";
import { buildFromAndWhere } from "../processor/buildFromAndWhere";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";

export class TriggerBuilder extends AbstractTriggerBuilder {

    createTrigger() {
        const triggerName = [
            "cache",
            this.cache.name,
            "for",
            this.cache.for.table.name,
            "on",
            this.triggerTable.name
        ].join("_");

        const func = new DatabaseFunction({
            schema: "public",
            name: triggerName,
            body: "\n" + this.createBody().toSQL() + "\n",
            comment: "cache",
            args: [],
            returns: {type: "trigger"}
        });

        const updateOfColumns = this.triggerTableColumns
            .filter(column =>  column !== "id" )
            .sort();
        
        const trigger = new DatabaseTrigger({
            name: triggerName,
            after: true,
            insert: true,
            delete: true,
            update: updateOfColumns.length > 0,
            updateOf: updateOfColumns,
            procedure: {
                schema: "public",
                name: triggerName,
                args: []
            },
            table: {
                schema: this.triggerTable.schema || "public",
                name: this.triggerTable.name
            }
        });

        return {
            trigger,
            function: func
        };
    }

    private createBody() {
        const referenceMeta = buildReferenceMeta(
            this.cache,
            this.triggerTable
        );
        const {from, where} = buildFromAndWhere(
            this.cache,
            this.triggerTable
        );

        if ( from.length === 1 ) {
            const optimizedBody = this.createOptimizedBody(
                referenceMeta
            );
            return optimizedBody;
        }

        const universalBody = buildUniversalBody({
            triggerTable: this.triggerTable,
            forTable: this.cache.for.toString(),
            updateColumns: this.cache.select.columns
                .map(col => col.name),
            select: this.cache.select.toString(),

            from,
            where,
            triggerTableColumns: this.triggerTableColumns
        });
        return universalBody;
    }

    private createOptimizedBody(referenceMeta: IReferenceMeta) {
        const mutableColumns = this.triggerTableColumns
            .filter(col => col !== "id");
        
        const joins = findJoinsMeta(this.cache.select);

        const whereOld = buildSimpleWhere(
            this.cache,
            this.triggerTable,
            "old",
            referenceMeta
        );
        const whereNew = buildSimpleWhere(
            this.cache,
            this.triggerTable,
            "new",
            referenceMeta
        );
        const noChanges = isNotDistinctFrom(mutableColumns);

        if ( joins.length ) {
            const oldJoins = buildJoins(joins, "old");
            const newJoins = buildJoins(joins, "new");
            
            const bodyWithJoins = buildCommutativeBodyWithJoins(
                noChanges,
                {
                    hasReference: hasReference(this.triggerTable, referenceMeta, "old"),
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
                    hasReference: hasReference(this.triggerTable, referenceMeta, "new"),
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
                    hasReference: hasReference(this.triggerTable, referenceMeta, "new"),
                    needUpdate: noReferenceChanges(
                        referenceMeta,
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
        else {

            const mutableColumnsDepsInAggregations = mutableColumns
                .filter(col => 
                    !referenceMeta.columns.includes(col)
                );

            const body = buildCommutativeBody(
                mutableColumns,
                noChanges,
                {
                    needUpdate: buildNeedUpdateCondition(
                        this.cache,
                        this.triggerTable,
                        referenceMeta,
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
                        referenceMeta,
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
                        referenceMeta,
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
}