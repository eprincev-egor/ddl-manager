import {
    Table,
    Cache
} from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { CommutativeTriggerBuilder } from "./CommutativeTriggerBuilder";
import { Database as DatabaseStructure } from "../schema/Database";
import { buildFrom } from "../processor/buildFrom";
import { UniversalTriggerBuilder } from "./UniversalTriggerBuilder";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { JoinedCommutativeTriggerBuilder } from "./JoinedCommutativeTriggerBuilder";
import { CacheContext } from "./CacheContext";

export class TriggerBuilderFactory {
    private readonly cache: Cache;
    private readonly databaseStructure: DatabaseStructure;

    constructor(
        cache: Cache,
        databaseStructure: DatabaseStructure,
    ) {
        this.cache = cache;
        this.databaseStructure = databaseStructure;
    }

    tryCreateBuilder(
        triggerTable: Table,
        triggerTableColumns: string[]
    ): AbstractTriggerBuilder | undefined {

        const context = new CacheContext(
            this.cache,
            triggerTable,
            triggerTableColumns,
            this.databaseStructure
        );

        const Builder = this.chooseConstructor(context);
        if ( Builder ) {
            const builder = new Builder(
                context
            );
            return builder;
        }
    }

    private chooseConstructor(context: CacheContext) {

        const from = buildFrom(context);
        const joins = findJoinsMeta(this.cache.select);

        if ( from.length > 1 ) {
            return UniversalTriggerBuilder;
        }
        else {
            const isTriggerOnCacheTable = context.triggerTable.equal(context.cache.for.table);
            const noDepsToCacheTable = context.cache.select
                .getAllTableReferences()
                .every(tableRef =>
                    !tableRef.table.equal(context.cache.for.table)
                );
            
            if ( isTriggerOnCacheTable && noDepsToCacheTable ) {
                return;
            }


            if ( joins.length ) {
                return JoinedCommutativeTriggerBuilder;
            }
            else {
                return CommutativeTriggerBuilder;
            }
        }
    }
}