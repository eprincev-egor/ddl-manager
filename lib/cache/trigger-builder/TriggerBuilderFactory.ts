import { AbstractExpressionElement, Cache, Expression } from "../../ast";
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
        const isTriggerOnCacheTable = context.triggerTable.equal(context.cache.for.table);
        const hasAgg = context.cache.select.columns.some(column => 
            column.getAggregations(context.database).length > 0
        );
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
        const arrayReference = context.referenceMeta.expressions.some(expression =>
            isArrayReference(context, expression)
        );

        if ( needUniversalTrigger ) {
            return UniversalTriggerBuilder;
        }
        else if ( hasAgg ) {
            const noDepsToCacheTable = context.cache.select
                .getAllTableReferences()
                .every(tableRef =>
                    !tableRef.table.equal(context.cache.for.table)
                );
            
            if ( isTriggerOnCacheTable && noDepsToCacheTable ) {
                return;
            }

            if ( arrayReference ) {
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

            if ( arrayReference ) {
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

function isArrayReference(
    context: CacheContext,
    expression: Expression
): boolean {
    const hasArrayOperator = (
        expression.isBinary("&&") ||
        expression.isBinary("@>") ||
        expression.isBinary("<@") ||
        expression.isEqualAny()
    );
    if ( !hasArrayOperator ) {
        return false;
    }

    const triggerColumns = expression.getColumnReferences().filter(columnRef =>
        context.isColumnRefToTriggerTable(columnRef)
    );
    const hasMutableTriggerColumn = triggerColumns.some(columnRef =>
        columnRef.name !== "id"
    );
    return hasMutableTriggerColumn;
}
