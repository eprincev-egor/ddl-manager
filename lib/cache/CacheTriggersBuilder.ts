import { CacheParser } from "../parser";
import {
    Cache
} from "../ast";
import { findDependencies } from "./processor/findDependencies";
import { Database } from "../database/schema/Database";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { TableID } from "../database/schema/TableID";
import { TriggerBuilderFactory } from "./trigger-builder/TriggerBuilderFactory";
import { createSelectForUpdate } from "./processor/createSelectForUpdate";

export class CacheTriggersBuilder {

    private readonly cache: Cache;
    private readonly builderFactory: TriggerBuilderFactory;

    constructor(
        cacheOrSQL: string | Cache,
        database: Database
    ) {
        let cache: Cache = cacheOrSQL as Cache;
        if ( typeof cacheOrSQL === "string" ) {
            cache = CacheParser.parse(cacheOrSQL);
        }
        this.cache = cache;
        this.builderFactory = new TriggerBuilderFactory(
            cache,
            database
        );
    }

    createSelectForUpdate() {
        const select = createSelectForUpdate(this.cache);
        return select;
    }

    createTriggers() {
        const output: {
            [tableName: string]: {
                trigger: DatabaseTrigger,
                function: DatabaseFunction
            };
        } = {};

        const allDeps = findDependencies(this.cache);

        for (const schemaTable of this.cache.withoutTriggers) {
            if ( !(schemaTable in allDeps) ) {
                throw new Error(`unknown table to ignore triggers: ${schemaTable}`);
            }
        }

        for (const schemaTable in allDeps) {
            const needIgnore = this.cache.withoutTriggers.includes(schemaTable);
            if ( needIgnore ) {
                continue;
            }

            const [schemaName, tableName] = schemaTable.split(".");

            const triggerTable = new TableID(schemaName, tableName);
            const tableDeps = allDeps[ schemaTable ];

            const triggerBuilder = this.builderFactory.tryCreateBuilder(
                triggerTable,
                tableDeps.columns
            );

            if ( triggerBuilder ) {
                const trigger = triggerBuilder.createTrigger();
                output[ schemaTable ] = trigger;
            }
        }

        return output;
    }

}
