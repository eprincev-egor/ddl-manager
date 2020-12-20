import { Cache } from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { CommutativeTriggerBuilder } from "./CommutativeTriggerBuilder";
import { Database } from "../../database/schema/Database";
import { TableID } from "../../database/schema/TableID";
import { buildFrom } from "../processor/buildFrom";
import { UniversalTriggerBuilder } from "./UniversalTriggerBuilder";
import { CacheContext } from "./CacheContext";
import { SelfUpdateByOtherTablesTriggerBuilder } from "./SelfUpdateByOtherTablesTriggerBuilder";
import { SelfUpdateBySelfRowTriggerBuilder } from "./SelfUpdateBySelfRowTriggerBuilder";
import { flatMap } from "lodash";

export class TriggerBuilderFactory {
    private readonly cache: Cache;
    private readonly database: Database;

    constructor(
        cache: Cache,
        database: Database,
    ) {
        this.cache = cache;
        this.database = database;
    }

    tryCreateBuilder(
        triggerTable: TableID,
        triggerTableColumns: string[]
    ): AbstractTriggerBuilder | undefined {

        const context = new CacheContext(
            this.cache,
            triggerTable,
            triggerTableColumns,
            this.database
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

        const isTriggerForSelfUpdate = (
            // trigger on cache table
            context.cache.for.table.equal(context.triggerTable) &&
            // has mutable columns in deps
            context.triggerTableColumns.filter(col => col !== "id").length > 0 &&
            // no "from cache table"
            !context.cache.select.getAllTableReferences().some(tableRef =>
                tableRef.table.equal(context.cache.for.table)
            )
        );

        if ( isTriggerForSelfUpdate ) {
            if ( context.cache.select.from.length > 0 ) {
                return SelfUpdateByOtherTablesTriggerBuilder;
            }
            else {
                return SelfUpdateBySelfRowTriggerBuilder;
            }
        }

        const from = buildFrom(context);
        const joins = flatMap(context.cache.select.from, fromItem => fromItem.joins);
        const isFromJoin = joins.some(join =>
            join.table.table.equal(context.triggerTable)
        );

        if ( from.length > 1 || from.length === 1 && isFromJoin ) {
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

            return CommutativeTriggerBuilder;
        }
    }
}