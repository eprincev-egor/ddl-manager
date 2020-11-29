import { Diff } from "../Diff";
import { IDatabaseDriver } from "../database/interface";
import { FunctionsMigrator } from "./FunctionsMigrator";
import { TriggersMigrator } from "./TriggersMigrator";
import { MainCacheMigrator } from "./cache/MainCacheMigrator";

export class MainMigrator {
    private diff: Diff;
    private postgres: IDatabaseDriver;

    static async migrate(postgres: IDatabaseDriver, diff: Diff) {
        const migrator = new MainMigrator(postgres, diff);
        return await migrator.migrate();
    }

    static async migrateWithoutUpdateCacheColumns(postgres: IDatabaseDriver, diff: Diff) {
        const migrator = new MainMigrator(postgres, diff);
        return await migrator.migrateWithoutUpdateCacheColumns();
    }

    static async refreshCache(postgres: IDatabaseDriver, diff: Diff) {
        const migrator = new MainMigrator(postgres, diff);
        return await migrator.refreshCache();
    }
    
    private constructor(postgres: IDatabaseDriver, diff: Diff) {
        this.postgres = postgres;
        this.diff = diff;
    }

    private async migrate() {
        const {
            functions,
            triggers,
            cache,
            outputErrors
        } = await this.createMigrators();

        await triggers.drop();
        await functions.drop();
        await cache.drop();

        await functions.create();
        await triggers.create();
        await cache.create();

        return outputErrors;
    }

    private async migrateWithoutUpdateCacheColumns() {
        const {
            functions,
            triggers,
            cache,
            outputErrors
        } = await this.createMigrators();

        await triggers.drop();
        await functions.drop();
        await cache.drop();

        await functions.create();
        await triggers.create();
        await cache.createWithoutUpdateCacheColumns();

        return outputErrors;
    }

    private async refreshCache() {
        const {
            cache,
            outputErrors
        } = await this.createMigrators();

        await cache.drop();
        await cache.create();

        return outputErrors;
    }

    private async createMigrators() {

        const databaseStructure = await this.postgres.load();
        const outputErrors: Error[] = [];

        const functions = new FunctionsMigrator(
            this.postgres,
            this.diff,
            databaseStructure,
            outputErrors
        );
        const triggers = new TriggersMigrator(
            this.postgres,
            this.diff,
            databaseStructure,
            outputErrors
        );
        const cache = new MainCacheMigrator(
            this.postgres,
            this.diff,
            databaseStructure,
            outputErrors
        );

        return {
            functions,
            triggers,
            cache,
            outputErrors
        };
    }
}
