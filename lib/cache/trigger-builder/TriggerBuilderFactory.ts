import { Cache } from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { CommutativeTriggerBuilder } from "./CommutativeTriggerBuilder";
import { Database } from "../../database/schema/Database";
import { TableID } from "../../database/schema/TableID";
import { buildFrom } from "../processor/buildFrom";
import { UniversalTriggerBuilder } from "./UniversalTriggerBuilder";
import { CacheContext } from "./CacheContext";
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
        const from = buildFrom(context);
        const joins = flatMap(context.cache.select.from, fromItem => fromItem.joins);
        const isFromJoin = joins.some(join =>
            join.table.table.equal(context.triggerTable)
        );
        const hasNotLeftJoin = joins.some(join => 
            join.type !== "left join"
        );

        const needUniversalTrigger = (
            hasNotLeftJoin || 
            from.length > 1 || 
            from.length === 1 && isFromJoin
        );

        if ( needUniversalTrigger ) {
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