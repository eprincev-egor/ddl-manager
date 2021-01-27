import { CacheParser } from "../parser";
import {
    Cache
} from "../ast";
import {
    findDependencies,
    findDependenciesToCacheTable
} from "./processor/findDependencies";
import { Database } from "../database/schema/Database";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { TableID } from "../database/schema/TableID";
import { TriggerBuilderFactory } from "./trigger-builder/TriggerBuilderFactory";
import { createSelectForUpdate } from "./processor/createSelectForUpdate";

export class CacheTriggersBuilder {

    private readonly cache: Cache;
    private readonly builderFactory: TriggerBuilderFactory;
    private readonly database: Database;

    constructor(
        cacheOrSQL: string | Cache,
        database: Database
    ) {
        let cache: Cache = cacheOrSQL as Cache;
        if ( typeof cacheOrSQL === "string" ) {
            cache = CacheParser.parse(cacheOrSQL);
        }
        this.database = database;
        this.cache = cache;
        this.builderFactory = new TriggerBuilderFactory(
            cache,
            database
        );
    }

    createSelectForUpdate() {
        const select = createSelectForUpdate(this.database, this.cache);
        return select;
    }

    createTriggers() {
        interface IOutputTrigger {
            name: string;
            table: TableID;
            trigger: DatabaseTrigger;
            function: DatabaseFunction;
        };
        const output: IOutputTrigger[] = [];

        const allDeps = findDependencies(this.cache);

        for (const schemaTable of this.cache.withoutTriggers) {
            if ( !(schemaTable in allDeps) ) {
                throw new Error(`unknown table to ignore triggers: ${schemaTable}`);
            }
        }

        const needIgnoreTriggerOnCacheTable = this.cache.withoutTriggers.includes(
            this.cache.for.table.toString()
        );
        if ( !needIgnoreTriggerOnCacheTable ) {
            const cacheTableDeps = findDependenciesToCacheTable(this.cache);

            const triggerBuilder = this.builderFactory.tryCreateBuilder(
                this.cache.for.table,
                cacheTableDeps.columns
            );

            if ( triggerBuilder ) {
                const {trigger, function: func} = triggerBuilder.createTrigger();
                output.push({
                    name: trigger.name,
                    table: this.cache.for.table,
                    trigger,
                    function: func
                });
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
                const {trigger, function: func} = triggerBuilder.createTrigger();
                output.push({
                    name: trigger.name,
                    table: triggerTable,
                    trigger: trigger,
                    function: func
                });
            }
        }

        return output;
    }

}
