import { CacheParser } from "../parser";
import {
    Cache, Select
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
import { SelfUpdateByOtherTablesTriggerBuilder } from "./trigger-builder/SelfUpdateByOtherTablesTriggerBuilder";
import { CacheContext } from "./trigger-builder/CacheContext";
import { SelfUpdateBySelfRowTriggerBuilder } from "./trigger-builder/SelfUpdateBySelfRowTriggerBuilder";

export interface ISelectForUpdate {
    for: TableID;
    select: Select;
}

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

    createSelectsForUpdate(): ISelectForUpdate[] {
        const select = createSelectForUpdate(this.database, this.cache);
        return [{
            for: this.cache.for.table,
            select
        }];
    }

    createTriggers() {
        interface IOutputTrigger {
            name: string;
            table: TableID;
            trigger: DatabaseTrigger;
            function: DatabaseFunction;
        };
        const output: IOutputTrigger[] = [];

        const allDeps = findDependencies(this.cache, false);

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
            const mutableColumns = cacheTableDeps.columns.filter(col =>
                col != "id"
            );
            const context = new CacheContext(
                this.cache,
                this.cache.for.table,
                mutableColumns,
                this.database,
                false
            );

            let TriggerBuilderConstructor: (
                typeof SelfUpdateBySelfRowTriggerBuilder |
                typeof SelfUpdateByOtherTablesTriggerBuilder |
                undefined
            );
            if ( this.cache.select.from.length === 0 ) {
                TriggerBuilderConstructor = SelfUpdateBySelfRowTriggerBuilder;
            }
            else if ( mutableColumns.length ) {
                TriggerBuilderConstructor = SelfUpdateByOtherTablesTriggerBuilder;
            }

            if ( TriggerBuilderConstructor ) {
                const builder = new TriggerBuilderConstructor(context);
                const {trigger, function: func} = builder.createTrigger();
    
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
            const tableDeps = allDeps[ schemaTable ];

            const triggerBuilder = this.builderFactory.tryCreateBuilder(
                new TableID(schemaName, tableName),
                tableDeps.columns
            );
            if ( !triggerBuilder ) {
                continue;
            }

            const result = triggerBuilder.createTrigger();
            output.push({
                name: result.trigger.name,
                table: result.trigger.table,
                trigger: result.trigger,
                function: result.function
            });
        }

        return output;
    }

}
