import { Migration } from "./Migration";
import { IDatabaseDriver } from "../database/interface";
import { FunctionsMigrator } from "./FunctionsMigrator";
import { TriggersMigrator } from "./TriggersMigrator";
import { MainCacheMigrator } from "./cache/MainCacheMigrator";

export class MainMigrator {
    private migration: Migration;
    private postgres: IDatabaseDriver;

    static async migrate(postgres: IDatabaseDriver, migration: Migration) {
        const migrator = new MainMigrator(postgres, migration);
        return await migrator.migrate();
    }

    static async migrateWithoutUpdateCacheColumns(postgres: IDatabaseDriver, migration: Migration) {
        const migrator = new MainMigrator(postgres, migration);
        return await migrator.migrateWithoutUpdateCacheColumns();
    }

    static async refreshCache(postgres: IDatabaseDriver, migration: Migration) {
        const migrator = new MainMigrator(postgres, migration);
        return await migrator.refreshCache();
    }
    
    private constructor(postgres: IDatabaseDriver, migration: Migration) {
        this.postgres = postgres;
        this.migration = migration;
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
            this.migration,
            databaseStructure,
            outputErrors
        );
        const triggers = new TriggersMigrator(
            this.postgres,
            this.migration,
            databaseStructure,
            outputErrors
        );
        const cache = new MainCacheMigrator(
            this.postgres,
            this.migration,
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
