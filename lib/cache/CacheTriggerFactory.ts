import { CacheParser } from "../parser";
import {
    Expression,
    Table,
    Cache,
    DatabaseFunction,
    DatabaseTrigger,
    SelectColumn
} from "../ast";
import { buildReferenceMeta, IReferenceMeta } from "./processor/buildReferenceMeta";
import { buildCommutativeBody } from "./processor/buildCommutativeBody";
import { buildCommutativeBodyWithJoins } from "./processor/buildCommutativeBodyWithJoins";
import { buildNeedUpdateCondition } from "./processor/buildNeedUpdateCondition";
import { hasEffect } from "./processor/hasEffect";
import { hasReference } from "./processor/hasReference";
import { buildUpdate } from "./processor/buildUpdate";
import { buildJoins } from "./processor/buildJoins";
import { buildUniversalBody } from "./processor/buildUniversalBody";
import { buildFromAndWhere } from "./processor/buildFromAndWhere";
import { findJoinsMeta } from "./processor/findJoinsMeta";
import { findDependencies } from "./processor/findDependencies";
import { buildSimpleWhere } from "./processor/buildSimpleWhere";
import { AggFactory } from "./aggregator";
import { flatMap } from "lodash";
import { Database as DatabaseStructure } from "./schema/Database";
import { Table as TableStructure } from "./schema/Table";
import { Column } from "./schema/Column";
import assert from "assert";

export class CacheTriggerFactory {

    createSelectForUpdate(cache: Cache) {
        const columnsToUpdate = flatMap(cache.select.columns, selectColumn => {
            const aggFactory = new AggFactory(cache.select, selectColumn);
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

        const selectToUpdate = cache.select.cloneWith({
            columns: columnsToUpdate
        });
        return selectToUpdate;
    }

    createTriggers(
        cacheOrSQL: string | Cache,
        databaseStructure: DatabaseStructure
    ) {
        const output: {
            [tableName: string]: {
                trigger: DatabaseTrigger,
                function: DatabaseFunction
            };
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
                    tableDeps.columns,
                    databaseStructure
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
        triggerTableColumns: string[],
        databaseStructure: DatabaseStructure
    ) {
        const triggerName = [
            "cache",
            cache.name,
            "for",
            cache.for.table.name,
            "on",
            triggerTable.name
        ].join("_");

        const func = new DatabaseFunction({
            schema: "public",
            name: triggerName,
            body: "\n" + this.createBody(
                cache,
                triggerTable,
                triggerTableColumns,
                databaseStructure
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
        cache: Cache,
        triggerTable: Table,
        triggerTableColumns: string[],
        databaseStructure: DatabaseStructure
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
                referenceMeta,
                databaseStructure
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
        referenceMeta: IReferenceMeta,
        databaseStructure: DatabaseStructure
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
                    needUpdate: noReferenceChanges(
                        referenceMeta,
                        triggerTable,
                        databaseStructure
                    ),
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
                    needUpdate: noReferenceChanges(
                        referenceMeta,
                        triggerTable,
                        databaseStructure
                    ),
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

function noReferenceChanges(
    referenceMeta: IReferenceMeta,
    triggerTable: Table,
    databaseStructure: DatabaseStructure
) {
    const importantColumns = referenceMeta.columns.slice();

    for (const filter of referenceMeta.filters) {
        const filterColumns = filter.getColumnReferences().map(columnRef =>
            columnRef.name
        );
        importantColumns.push( ...filterColumns );
    }

    const mutableImportantColumns = importantColumns.filter(column =>
        column !== "id"
    );

    const tableStructure = databaseStructure.getTable(triggerTable) as TableStructure;
    // assert.ok(tableStructure, `table ${ triggerTable.toString() } does not exists`);

    const conditions: string[] = [];
    for (const columnName of mutableImportantColumns) {
        const column = tableStructure && tableStructure.getColumn(columnName) as Column;
        // assert.ok(column, `column ${ triggerTable.toString() }.${ columnName } does not exists`);

        if ( column && column.type.isArray() ) {
            conditions.push(`not cm_is_distinct_arrays(new.${columnName}, old.${columnName})`);
        }
        else {
            conditions.push(`new.${ columnName } is not distinct from old.${ columnName }`);
        }
    }
    
    return Expression.and(conditions);
}
