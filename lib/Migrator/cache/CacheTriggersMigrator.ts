import { AbstractMigrator } from "../AbstractMigrator";
import { Cache } from "../../ast";
import { CacheTriggersBuilder } from "../../cache/CacheTriggersBuilder";
import { Database as DatabaseStructure } from "../../database/schema/Database";

export class CacheTriggersMigrator extends AbstractMigrator {
    async drop() {
        for (const cache of this.diff.drop.cache) {
            await this.dropCacheTriggers(cache);
        }
    }

    async create() {
        for (const cache of this.diff.create.cache || []) {
            await this.createCacheTriggers(cache);
        }
    }

    private async dropCacheTriggers(cache: Cache) {

        const cacheTriggerFactory = new CacheTriggersBuilder(
            cache,
            this.databaseStructure
        );
        
        const triggersByTableName = cacheTriggerFactory.createTriggers();
        for (const {trigger, function: func} of Object.values(triggersByTableName)) {

            try {
                await this.postgres.forceDropTrigger(trigger);
            } catch(err) {
                this.onError(cache, err);
            }
            
            try {
                await this.postgres.forceDropFunction(func);
            } catch(err) {
                this.onError(cache, err);
            }
        }
    }

    private async createCacheTriggers(cache: Cache) {
        const cacheTriggerFactory = new CacheTriggersBuilder(
            cache,
            this.databaseStructure
        );
        const triggersByTableName = cacheTriggerFactory.createTriggers();

        for (const tableName in triggersByTableName) {
            const {trigger, function: func} = triggersByTableName[ tableName ];

            try {
                await this.postgres.createOrReplaceCacheTrigger(
                    trigger,
                    func
                );
            } catch(err) {
                this.onError(cache, err);
            }
        }
    }
}