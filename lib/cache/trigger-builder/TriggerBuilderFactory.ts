import { Cache } from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { CommutativeTriggerBuilder } from "./commutative/CommutativeTriggerBuilder";
import { ArrayRefCommutativeTriggerBuilder } from "./commutative/ArrayRefCommutativeTriggerBuilder";
import { Database } from "../../database/schema/Database";
import { TableID } from "../../database/schema/TableID";
import { buildFrom } from "../processor/buildFrom";
import { UniversalTriggerBuilder } from "./UniversalTriggerBuilder";
import { CacheContext } from "./CacheContext";
import { flatMap } from "lodash";
import { OneRowTriggerBuilder } from "./OneRowTriggerBuilder";
import { LastRowByIdTriggerBuilder } from "./one-last-row/LastRowByIdTriggerBuilder";
import { LastRowByMutableTriggerBuilder } from "./one-last-row/LastRowByMutableTriggerBuilder";
import { LastRowByArrayReferenceTriggerBuilder } from "./one-last-row/LastRowByArrayReferenceTriggerBuilder";
import { buildArrVars } from "../processor/buildArrVars";

export class TriggerBuilderFactory {
    private readonly allCache: Cache[];
    private readonly cache: Cache;
    private readonly database: Database;

    constructor(
        allCache: Cache[],
        cache: Cache,
        database: Database,
    ) {
        this.allCache = allCache;
        this.cache = cache;
        this.database = database;
    }

    tryCreateBuilder(
        triggerTable: TableID,
        triggerTableColumns: string[]
    ): AbstractTriggerBuilder | undefined {

        const context = new CacheContext(
            this.allCache,
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
        const isTriggerOnCacheTable = context.triggerTable.equal(context.cache.for.table);
        const joins = flatMap(context.cache.select.from, fromItem => fromItem.joins);
        const isFromJoin = joins.some(join =>
            join.getTableId().equal(context.triggerTable)
        );
        const hasNotLeftJoin = joins.some(join => 
            join.type !== "left join"
        );

        const needUniversalTrigger = (
            hasNotLeftJoin || 
            from.length > 1 || 
            from.length === 1 && isFromJoin
        );
        const arrayVars = buildArrVars(context);

        if ( needUniversalTrigger ) {
            return UniversalTriggerBuilder;
        }
        else if ( context.hasAgg() ) {
            const noDepsToCacheTable = context.cache.select
                .getAllTableReferences()
                .every(tableRef =>
                    !tableRef.table.equal(context.cache.for.table)
                );
            
            if ( isTriggerOnCacheTable && noDepsToCacheTable ) {
                return;
            }

            if ( arrayVars.length ) {
                return ArrayRefCommutativeTriggerBuilder;
            }
            else {
                return CommutativeTriggerBuilder;
            }
        }
        else if ( this.oneLastRow(context) ) {
            const orderBy = context.cache.select.orderBy!;
            const orderByColumns = orderBy.getColumnReferences();
            const firstOrderColumn = orderByColumns[0];

            if ( arrayVars.length ) {
                return LastRowByArrayReferenceTriggerBuilder;
            }

            const byId = (
                orderByColumns.length === 1 &&
                firstOrderColumn.name === "id" &&
                context.isColumnRefToTriggerTable( firstOrderColumn )
            );
            if ( byId ) {
                return LastRowByIdTriggerBuilder;
            }
            else {
                return LastRowByMutableTriggerBuilder;
            }
        }
        else if ( this.oneRowFromTable(context) ) {
            return OneRowTriggerBuilder;
        }
    }

    private oneRowFromTable(context: CacheContext) {
        return (
            context.cache.select.from.length === 1 &&
            context.referenceMeta.expressions.length > 0
        );
    }

    private oneLastRow(context: CacheContext) {
        const {select} = context.cache;
        return (
            select.from.length === 1 &&
            select.orderBy &&
            select.limit === 1 &&
            context.referenceMeta.expressions.length > 0
        );
    }
}
