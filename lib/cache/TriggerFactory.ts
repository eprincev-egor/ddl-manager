import { CacheParser } from "../parser";
import {
    CreateTrigger,
    CreateFunction,
    Expression,
    Table,
    Cache
} from "../ast";
import { buildReferenceMeta, IReferenceMeta } from "./processor/buildReferenceMeta";
import { buildCommutativeBody } from "./processor/buildCommutativeBody";
import { buildCommutativeBodyWithJoins } from "./processor/buildCommutativeBodyWithJoins";
import { buildNeedUpdateCondition } from "./processor/buildNeedUpdateCondition";
import { hasEffect } from "./processor/hasEffect";
import { hasReference } from "./processor/hasReference";
import { isNotDistinctFrom } from "./processor/isNotDistinctFrom";
import { buildUpdate } from "./processor/buildUpdate";
import { buildJoins } from "./processor/buildJoins";
import { buildUniversalBody } from "./processor/buildUniversalBody";
import { buildFromAndWhere } from "./processor/buildFromAndWhere";
import { findJoinsMeta } from "./processor/findJoinsMeta";
import { findDependencies } from "./processor/findDependencies";
import { buildSimpleWhere } from "./processor/buildSimpleWhere";

export class TriggerFactory {

    createTriggers(cacheOrSQL: string | Cache) {
        const output: {
            [tableName: string]: CreateTrigger;
        } = {};

        let cache: Cache = cacheOrSQL as Cache;
        if ( typeof cacheOrSQL === "string" ) {
            cache = CacheParser.parse(cacheOrSQL);
        }

        const allDeps = findDependencies(cache);

        for (const schemaTable in allDeps) {
            const [schemaName, tableName] = schemaTable.split(".");

            const triggerTable = new Table(schemaName, tableName);
            const tableDeps = allDeps[ schemaTable ];

            try {
                output[ schemaTable ] = this.createTrigger(
                    cache,
                    triggerTable,
                    tableDeps.columns
                );
            } catch(err) {
                if ( /no [\w\.]+ in select/.test(err.message) ) {
                    continue;
                }
                throw err;
            }
        }

        return output;
    }

    private createTrigger(
        cache: Cache,
        triggerTable: Table,
        triggerTableColumns: string[]
    ) {
        return new CreateTrigger({
            function: new CreateFunction({
                name: [
                    "cache",
                    cache.name,
                    "for",
                    cache.for.table.name,
                    "on",
                    triggerTable.name
                ].join("_"),
                body: this.createBody(
                    cache,
                    triggerTable,
                    triggerTableColumns
                )
            }),
            table: triggerTable.toString(),
            columns: triggerTableColumns
                .filter(column => 
                    column !== "id"
                )
                .sort()
        });
    }

    private createBody(
        cache: Cache,
        triggerTable: Table,
        triggerTableColumns: string[]
    ) {
        const referenceMeta = buildReferenceMeta(
            cache,
            triggerTable
        );
        const {from, where} = buildFromAndWhere(cache, triggerTable);

        if ( from.length === 1 ) {
            const optimizedBody = this.createOptimizedBody(
                cache,
                triggerTable,
                triggerTableColumns,
                referenceMeta
            );
            return optimizedBody;
        }

        const universalBody = buildUniversalBody({
            triggerTable,
            forTable: cache.for.toString(),
            updateColumns: cache.select.columns
                .map(col => col.name),
            select: cache.select.toString(),

            from,
            where,
            triggerTableColumns
        });
        return universalBody;
    }

    private createOptimizedBody(
        cache: Cache,
        triggerTable: Table,
        triggerTableColumns: string[],
        referenceMeta: IReferenceMeta
    ) {
        const mutableColumns = triggerTableColumns
            .filter(col => col !== "id");
        
        const joins = findJoinsMeta(cache.select);

        const whereOld = buildSimpleWhere(
            cache,
            triggerTable,
            "old",
            referenceMeta
        );
        const whereNew = buildSimpleWhere(
            cache,
            triggerTable,
            "new",
            referenceMeta
        );

        if ( joins.length ) {
            const oldJoins = buildJoins(joins, "old");
            const newJoins = buildJoins(joins, "new");
            
            const bodyWithJoins = buildCommutativeBodyWithJoins(
                mutableColumns,
                {
                    hasReference: hasReference(triggerTable, referenceMeta, "old"),
                    needUpdate: hasEffect(
                        cache,
                        triggerTable,
                        "old",
                        joins
                    ) as Expression,
                    update: buildUpdate(
                        cache,
                        triggerTable,
                        whereOld,
                        joins,
                        "minus"
                    ),
                    joins: oldJoins
                },
                {
                    hasReference: hasReference(triggerTable, referenceMeta, "new"),
                    needUpdate: hasEffect(
                        cache,
                        triggerTable,
                        "new",
                        joins
                    ) as Expression,
                    update: buildUpdate(
                        cache,
                        triggerTable,
                        whereNew,
                        joins,
                        "plus"
                    ),
                    joins: newJoins
                },
                {
                    hasReference: hasReference(triggerTable, referenceMeta, "new"),
                    needUpdate: noReferenceChanges(referenceMeta),
                    update: buildUpdate(
                        cache,
                        triggerTable,
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
                {
                    needUpdate: buildNeedUpdateCondition(
                        cache,
                        triggerTable,
                        referenceMeta,
                        "old"
                    ),
                    update: buildUpdate(
                        cache,
                        triggerTable,
                        whereOld,
                        joins,
                        "minus"
                    )
                },
                {
                    needUpdate: buildNeedUpdateCondition(
                        cache,
                        triggerTable,
                        referenceMeta,
                        "new"
                    ),
                    update: buildUpdate(
                        cache,
                        triggerTable,
                        whereNew,
                        joins,
                        "plus"
                    )
                },
                mutableColumnsDepsInAggregations.length ? {
                    needUpdate: noReferenceChanges(referenceMeta),
                    update: buildUpdate(
                        cache,
                        triggerTable,
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

function noReferenceChanges(referenceMeta: IReferenceMeta) {
    const importantColumns = referenceMeta.columns.slice();

    for (const filter of referenceMeta.filters) {
        const filterColumns = filter.getColumnReferences().map(columnRef =>
            columnRef.name
        );
        importantColumns.push( ...filterColumns );
    }

    return isNotDistinctFrom(importantColumns);
}
