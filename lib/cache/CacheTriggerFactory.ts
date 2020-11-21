import { CacheParser } from "../parser";
import {
    Expression,
    Table,
    Cache,
    DatabaseFunction,
    DatabaseTrigger,
    SelectColumn
} from "../ast";
import { buildReferenceMeta, IReferenceMeta } from "./processor/condition/buildReferenceMeta";
import { noReferenceChanges } from "./processor/condition/noReferenceChanges";
import { buildCommutativeBody } from "./processor/buildCommutativeBody";
import { buildNeedUpdateCondition } from "./processor/condition/buildNeedUpdateCondition";
import { hasEffect } from "./processor/condition/hasEffect";
import { hasReference } from "./processor/condition/hasReference";
import { buildSimpleWhere } from "./processor/condition/buildSimpleWhere";
import { isNotDistinctFrom } from "./processor/condition/isNotDistinctFrom";

import { buildCommutativeBodyWithJoins } from "./processor/buildCommutativeBodyWithJoins";
import { buildUpdate } from "./processor/buildUpdate";
import { buildJoins } from "./processor/buildJoins";
import { buildUniversalBody } from "./processor/buildUniversalBody";
import { buildFromAndWhere } from "./processor/buildFromAndWhere";
import { findJoinsMeta } from "./processor/findJoinsMeta";
import { findDependencies } from "./processor/findDependencies";
import { AggFactory } from "./aggregator";
import { flatMap } from "lodash";
import { Database as DatabaseStructure } from "./schema/Database";

export class CacheTriggerFactory {

    private readonly cache: Cache;
    private readonly databaseStructure: DatabaseStructure;

    constructor(
        cacheOrSQL: string | Cache,
        databaseStructure: DatabaseStructure
    ) {
        let cache: Cache = cacheOrSQL as Cache;
        if ( typeof cacheOrSQL === "string" ) {
            cache = CacheParser.parse(cacheOrSQL);
        }
        this.cache = cache;
        this.databaseStructure = databaseStructure;
    }

    createSelectForUpdate() {
        const columnsToUpdate = flatMap(this.cache.select.columns, selectColumn => {
            const aggFactory = new AggFactory(this.cache.select, selectColumn);
            const aggregations = aggFactory.createAggregations();
            
            const columns = Object.keys(aggregations).map(aggColumnName => {
                const agg = aggregations[ aggColumnName ];

                let expression = new Expression([
                    agg.call
                ]);
                if ( agg.call.name === "sum" ) {
                    expression = Expression.funcCall("coalesce", [
                        expression,
                        Expression.unknown( agg.default() )
                    ]);
                }

                const column = new SelectColumn({
                    name: aggColumnName,
                    expression
                });
                return column;
            });

            if ( !selectColumn.expression.isFuncCall() ) {
                columns.push(selectColumn);
            }

            return columns;
        });

        const selectToUpdate = this.cache.select.cloneWith({
            columns: columnsToUpdate
        });
        return selectToUpdate;
    }

    createTriggers() {
        const output: {
            [tableName: string]: {
                trigger: DatabaseTrigger,
                function: DatabaseFunction
            };
        } = {};

        const allDeps = findDependencies(this.cache);

        for (const schemaTable in allDeps) {
            const [schemaName, tableName] = schemaTable.split(".");

            const triggerTable = new Table(schemaName, tableName);
            const tableDeps = allDeps[ schemaTable ];

            try {
                output[ schemaTable ] = this.createTrigger(
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
        triggerTable: Table,
        triggerTableColumns: string[]
    ) {
        const triggerName = [
            "cache",
            this.cache.name,
            "for",
            this.cache.for.table.name,
            "on",
            triggerTable.name
        ].join("_");

        const func = new DatabaseFunction({
            schema: "public",
            name: triggerName,
            body: "\n" + this.createBody(
                triggerTable,
                triggerTableColumns
            ).toSQL() + "\n",
            comment: "cache",
            args: [],
            returns: {type: "trigger"}
        });

        const updateOfColumns = triggerTableColumns
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
                schema: triggerTable.schema || "public",
                name: triggerTable.name
            }
        });

        return {
            trigger,
            function: func
        };
    }

    private createBody(
        triggerTable: Table,
        triggerTableColumns: string[]
    ) {
        const referenceMeta = buildReferenceMeta(
            this.cache,
            triggerTable
        );
        const {from, where} = buildFromAndWhere(this.cache, triggerTable);

        if ( from.length === 1 ) {
            const optimizedBody = this.createOptimizedBody(
                triggerTable,
                triggerTableColumns,
                referenceMeta
            );
            return optimizedBody;
        }

        const universalBody = buildUniversalBody({
            triggerTable,
            forTable: this.cache.for.toString(),
            updateColumns: this.cache.select.columns
                .map(col => col.name),
            select: this.cache.select.toString(),

            from,
            where,
            triggerTableColumns
        });
        return universalBody;
    }

    private createOptimizedBody(
        triggerTable: Table,
        triggerTableColumns: string[],
        referenceMeta: IReferenceMeta
    ) {
        const mutableColumns = triggerTableColumns
            .filter(col => col !== "id");
        
        const joins = findJoinsMeta(this.cache.select);

        const whereOld = buildSimpleWhere(
            this.cache,
            triggerTable,
            "old",
            referenceMeta
        );
        const whereNew = buildSimpleWhere(
            this.cache,
            triggerTable,
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
                    hasReference: hasReference(triggerTable, referenceMeta, "old"),
                    needUpdate: hasEffect(
                        this.cache,
                        triggerTable,
                        "old",
                        joins
                    ) as Expression,
                    update: buildUpdate(
                        this.cache,
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
                        this.cache,
                        triggerTable,
                        "new",
                        joins
                    ) as Expression,
                    update: buildUpdate(
                        this.cache,
                        triggerTable,
                        whereNew,
                        joins,
                        "plus"
                    ),
                    joins: newJoins
                },
                {
                    hasReference: hasReference(triggerTable, referenceMeta, "new"),
                    needUpdate: noReferenceChanges(
                        referenceMeta,
                        triggerTable,
                        this.databaseStructure
                    ),
                    update: buildUpdate(
                        this.cache,
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
                noChanges,
                {
                    needUpdate: buildNeedUpdateCondition(
                        this.cache,
                        triggerTable,
                        referenceMeta,
                        "old"
                    ),
                    update: buildUpdate(
                        this.cache,
                        triggerTable,
                        whereOld,
                        joins,
                        "minus"
                    )
                },
                {
                    needUpdate: buildNeedUpdateCondition(
                        this.cache,
                        triggerTable,
                        referenceMeta,
                        "new"
                    ),
                    update: buildUpdate(
                        this.cache,
                        triggerTable,
                        whereNew,
                        joins,
                        "plus"
                    )
                },
                mutableColumnsDepsInAggregations.length ? {
                    needUpdate: noReferenceChanges(
                        referenceMeta,
                        triggerTable,
                        this.databaseStructure
                    ),
                    update: buildUpdate(
                        this.cache,
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
