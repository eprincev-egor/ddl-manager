import { CacheParser } from "../parser";
import { Cache, Select } from "../ast";
import { findDependencies, findDependenciesToCacheTable } from "./processor/findDependencies";
import { Database } from "../database/schema/Database";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { TableID } from "../database/schema/TableID";
import { TriggerBuilderFactory } from "./trigger-builder/TriggerBuilderFactory";
import { SelfUpdateByOtherTablesTriggerBuilder } from "./trigger-builder/SelfUpdateByOtherTablesTriggerBuilder";
import { CacheContext } from "./trigger-builder/CacheContext";
import { SelfUpdateBySelfRowTriggerBuilder } from "./trigger-builder/SelfUpdateBySelfRowTriggerBuilder";
import { TableReference } from "../database/schema/TableReference";
import { FilesState } from "../fs/FilesState";
import { CacheColumnGraph } from "../Comparator/graph/CacheColumnGraph";

export interface ISelectForUpdate {
    for: TableReference;
    select: Select;
}

export interface IOutputTrigger {
    name: string;
    trigger: DatabaseTrigger;
    function: DatabaseFunction;
};

export class CacheTriggersBuilder {

    private readonly allCache: Cache[];
    private readonly cache: Cache;
    private readonly builderFactory: TriggerBuilderFactory;
    private readonly database: Database;
    private readonly fs: FilesState;
    private readonly graph: CacheColumnGraph;

    constructor(
        allCache: Cache[],
        cacheOrSQL: string | Cache,
        database: Database = new Database(),
        graph: CacheColumnGraph = new CacheColumnGraph([]),
        fs: FilesState = new FilesState()
    ) {
        this.graph = graph;
        this.allCache = allCache;

        let cache: Cache = cacheOrSQL as Cache;
        if ( typeof cacheOrSQL === "string" ) {
            cache = CacheParser.parse(cacheOrSQL);
        }

        this.database = database;
        this.fs = fs;
        this.cache = cache;
        this.builderFactory = new TriggerBuilderFactory(
            this.allCache,
            cache,
            database,
            this.fs,
            this.graph
        );
    }

    createTriggers() {
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
                this.fs,
                this.graph,
                false
            );

            let TriggerBuilderConstructor: (
                typeof SelfUpdateBySelfRowTriggerBuilder |
                typeof SelfUpdateByOtherTablesTriggerBuilder |
                undefined
            );
            if ( !this.cache.hasForeignTablesDeps() ) {
                TriggerBuilderConstructor = SelfUpdateBySelfRowTriggerBuilder;
            }
            else {
                TriggerBuilderConstructor = SelfUpdateByOtherTablesTriggerBuilder;
            }

            if ( TriggerBuilderConstructor ) {
                const builder = new TriggerBuilderConstructor(context);
                const triggers = builder.createTriggers();

                for (const {trigger, procedure} of triggers) {
                    output.push({
                        name: trigger.name,
                        trigger: trigger,
                        function: procedure
                    });
                }
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

            const triggers = triggerBuilder.createTriggers();
            for (const {trigger, procedure} of triggers) {
                output.push({
                    name: trigger.name,
                    trigger: trigger,
                    function: procedure
                });
            }
        }

        return output;
    }
}
