import { CacheParser } from "../parser";
import {
    Expression,
    Table,
    Cache,
    DatabaseFunction,
    DatabaseTrigger,
    SelectColumn
} from "../ast";
import { findDependencies } from "./processor/findDependencies";
import { AggFactory } from "./aggregator";
import { flatMap } from "lodash";
import { Database as DatabaseStructure } from "./schema/Database";
import { TriggerBuilderFactory } from "./trigger-builder/TriggerBuilderFactory";

export class CacheTriggersBuilder {

    private readonly cache: Cache;
    private readonly builderFactory: TriggerBuilderFactory;

    constructor(
        cacheOrSQL: string | Cache,
        databaseStructure: DatabaseStructure
    ) {
        let cache: Cache = cacheOrSQL as Cache;
        if ( typeof cacheOrSQL === "string" ) {
            cache = CacheParser.parse(cacheOrSQL);
        }
        this.cache = cache;
        this.builderFactory = new TriggerBuilderFactory(
            cache,
            databaseStructure
        );
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

            const triggerBuilder = this.builderFactory.createBuilder(
                triggerTable,
                tableDeps.columns
            );

            try {
                output[ schemaTable ] = triggerBuilder.createTrigger();
            } catch(err) {
                if ( /no [\w\.]+ in select/.test(err.message) ) {
                    continue;
                }
                throw err;
            }
        }

        return output;
    }

}
