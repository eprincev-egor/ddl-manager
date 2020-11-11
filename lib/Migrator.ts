import assert from "assert";
import { IDiff } from "./interface";
import { PostgresDriver } from "./database/PostgresDriver";
import { TriggerFactory } from "./cache/TriggerFactory";

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