import assert from "assert";
import { IDiff } from "./interface";
import { PostgresDriver } from "./database/PostgresDriver";
import { TriggerFactory } from "./cache/TriggerFactory";
import { Expression, FuncCall, SelectColumn } from "./ast";
import { AbstractAgg, AggFactory } from "./cache/aggregator";

export class Migrator {
    private postgres: PostgresDriver;

    constructor(postgres: PostgresDriver) {
        this.postgres = postgres;
    }

    async migrate(diff: IDiff) {
        assert.ok(diff);

        const outputErrors: Error[] = [];

        await this.dropTriggers(diff, outputErrors);
        await this.dropFunctions(diff, outputErrors);

        await this.createFunctions(diff, outputErrors);
        await this.createTriggers(diff, outputErrors);
        await this.createCache(diff, outputErrors);

        return outputErrors;
    }

    private async dropTriggers(diff: IDiff, outputErrors: Error[]) {

        for (const trigger of diff.drop.triggers) {
            try {
                await this.postgres.dropTrigger(trigger);
            } catch(err) {
                // redefine callstack
                const newErr = new Error(trigger.getSignature() + "\n" + err.message);
                (newErr as any).originalError = err;

                outputErrors.push(newErr);
            }
        }
    }

    private async dropFunctions(diff: IDiff, outputErrors: Error[]) {

        for (const func of diff.drop.functions) {
            // 2BP01
            try {
                await this.postgres.dropFunction(func);
            } catch(err) {
                // https://www.postgresql.org/docs/12/errcodes-appendix.html
                // cannot drop function my_func() because other objects depend on it
                const isCascadeError = err.code === "2BP01";
                if ( !isCascadeError ) {
                    // redefine callstack
                    const newErr = new Error(func.getSignature() + "\n" + err.message);
                    (newErr as any).originalError = err;

                    outputErrors.push(newErr);
                }
            }
        }
    }

    private async createFunctions(diff: IDiff, outputErrors: Error[]) {

        for (const func of diff.create.functions) {
            try {
                await this.postgres.createOrReplaceFunction(func);
            } catch(err) {
                // redefine callstack
                const newErr = new Error(func.getSignature() + "\n" + err.message);
                (newErr as any).originalError = err;
                
                outputErrors.push(newErr);
            }
        }
    }

    private async createTriggers(diff: IDiff, outputErrors: Error[]) {

        for (const trigger of diff.create.triggers) {
            try {
                await this.postgres.createOrReplaceTrigger(trigger);
            } catch(err) {
                // redefine callstack
                const newErr = new Error(trigger.getSignature() + "\n" + err.message);
                (newErr as any).originalError = err;
                
                outputErrors.push(newErr);
            }
        }
    }

    private async createCache(diff: IDiff, outputErrors: Error[]) {

        const cacheTriggerFactory = new TriggerFactory();

        for (const cache of diff.create.cache || []) {
            
            // TODO: create helpers columns
            const columnsTypes = await this.postgres.getCacheColumnsTypes(
                cache.select,
                cache.for
            );
            for (const columnName in columnsTypes) {
                const columnType = columnsTypes[ columnName ];

                const selectColumn = cache.select.columns.find(column =>
                    column.name === columnName
                ) as SelectColumn;

                const aggFactory = new AggFactory(selectColumn);
                const aggregations = aggFactory.createAggregations();
                const agg = Object.values(aggregations)[0] as AbstractAgg;

                await this.postgres.createOrReplaceColumn(
                    cache.for.table,
                    {
                        key: columnName,
                        type: columnType,
                        // TODO: detect default by expression
                        default: agg.default()
                    }
                );
            }

            // TODO: update helpers columns
            await this.postgres.updateCachePackage(
                cache.select.cloneWith({
                    columns: cache.select.columns.map(selectColumn => {
                        const aggFactory = new AggFactory(selectColumn);
                        const aggregations = aggFactory.createAggregations();
                        const agg = Object.values(aggregations)[0] as AbstractAgg;

                        if ( agg.call.name !== "sum" ) {
                            return selectColumn;
                        }

                        const newExpression = Expression.funcCall("coalesce", [
                            selectColumn.expression,
                            Expression.unknown( agg.default() )
                        ]);
                        return selectColumn.replaceExpression(newExpression);
                    })
                }),
                cache.for,
                500
            );

            const triggersByTableName = cacheTriggerFactory.createTriggers(cache);

            for (const tableName in triggersByTableName) {
                const {trigger, function: func} = triggersByTableName[ tableName ];

                try {
                    await this.postgres.createOrReplaceFunction(func);
                    await this.postgres.createOrReplaceTrigger(trigger);
                } catch(err) {
                    // redefine callstack
                    const newErr = new Error(
                        `cache ${cache.name} for ${cache.for}\n${err.message}`
                    );
                    (newErr as any).originalError = err;
                    
                    outputErrors.push(newErr);
                }
            }
        }
    }
}