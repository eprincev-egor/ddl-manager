import assert from "assert";
import { CacheTriggerFactory } from "./cache/CacheTriggerFactory";
import { Expression, Cache, SelectColumn } from "./ast";
import { AbstractAgg, AggFactory } from "./cache/aggregator";
import { Diff } from "./Diff";
import { IDatabaseDriver } from "./database/interface";

export class Migrator {
    private postgres: IDatabaseDriver;
    private cacheTriggerFactory: CacheTriggerFactory;
    private outputErrors: Error[];
    private diff: Diff;

    static async migrate(postgres: IDatabaseDriver, diff: Diff) {
        assert.ok(diff);
        const migrator = new Migrator(postgres, diff);
        return await migrator.migrate();
    }
    
    private constructor(postgres: IDatabaseDriver, diff: Diff) {
        this.postgres = postgres;
        this.diff = diff;
        this.cacheTriggerFactory = new CacheTriggerFactory();
        this.outputErrors = [];
    }

    async migrate() {
        await this.dropTriggers();
        await this.dropFunctions();

        await this.createFunctions();
        await this.createTriggers();
        await this.createAllCache();

        return this.outputErrors;
    }

    private async dropTriggers() {

        for (const trigger of this.diff.drop.triggers) {
            try {
                await this.postgres.dropTrigger(trigger);
            } catch(err) {
                this.onError(trigger, err);
            }
        }
    }

    private async dropFunctions() {

        for (const func of this.diff.drop.functions) {
            // 2BP01
            try {
                await this.postgres.dropFunction(func);
            } catch(err) {
                // https://www.postgresql.org/docs/12/errcodes-appendix.html
                // cannot drop function my_func() because other objects depend on it
                const isCascadeError = err.code === "2BP01";
                if ( !isCascadeError ) {
                    this.onError(func, err);
                }
            }
        }
    }

    private async createFunctions() {

        for (const func of this.diff.create.functions) {
            try {
                await this.postgres.createOrReplaceFunction(func);
            } catch(err) {
                this.onError(func, err);
            }
        }
    }

    private async createTriggers() {

        for (const trigger of this.diff.create.triggers) {
            try {
                await this.postgres.createOrReplaceTrigger(trigger);
            } catch(err) {
                this.onError(trigger, err);
            }
        }
    }

    private async createAllCache() {
        for (const cache of this.diff.create.cache || []) {
            await this.createCache(cache);
        }
    }

    private async createCache(cache: Cache) {
        await this.createCacheColumns(cache);
        await this.updateCachePackage(cache);
        await this.createCacheTriggers(cache);
    }

    private async createCacheColumns(cache: Cache) {
        const selectToUpdate = this.cacheTriggerFactory.createSelectForUpdate(cache);
        const columnsTypes = await this.postgres.getCacheColumnsTypes(
            selectToUpdate,
            cache.for
        );
        for (const columnName in columnsTypes) {
            const columnType = columnsTypes[ columnName ];

            const selectColumn = cache.select.columns.find(column =>
                column.name === columnName
            ) as SelectColumn;

            const aggFactory = new AggFactory(cache.select, selectColumn);
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
    }

    private async updateCachePackage(cache: Cache) {
        const selectToUpdate = this.cacheTriggerFactory.createSelectForUpdate(cache);

        const limit = 500;
        let updatedCount = 0;

        do {
            updatedCount = await this.postgres.updateCachePackage(
                selectToUpdate,
                cache.for,
                limit
            );
        } while( updatedCount >= limit );
    }

    private async createCacheTriggers(cache: Cache) {
        const triggersByTableName = this.cacheTriggerFactory.createTriggers(cache);

        for (const tableName in triggersByTableName) {
            const {trigger, function: func} = triggersByTableName[ tableName ];

            try {
                await this.postgres.createOrReplaceFunction(func);
                await this.postgres.createOrReplaceTrigger(trigger);
            } catch(err) {
                this.onError(cache, err);
            }
        }
    }

    private onError(
        obj: {getSignature(): string},
        err: Error
    ) {
        // redefine callstack
        const newErr = new Error(obj.getSignature() + "\n" + err.message);
        (newErr as any).originalError = err;
        
        this.outputErrors.push(newErr);
    }
}
