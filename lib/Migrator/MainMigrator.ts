import { Migration } from "./Migration";
import { IDatabaseDriver } from "../database/interface";
import { FunctionsMigrator } from "./FunctionsMigrator";
import { TriggersMigrator } from "./TriggersMigrator";
import { ColumnsMigrator } from "./ColumnsMigrator";
import { UpdateMigrator } from "./UpdateMigrator";

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
            columns,
            updates,
            outputErrors
        } = await this.createMigrators();

        await triggers.drop();
        await functions.drop();
        await columns.drop();

        await columns.create();
        await updates.create();
        await functions.create();
        await triggers.create();

        return outputErrors;
    }

    private async migrateWithoutUpdateCacheColumns() {
        const {
            functions,
            triggers,
            columns,
            outputErrors
        } = await this.createMigrators();

        await triggers.drop();
        await functions.drop();
        await columns.drop();

        await columns.create();
        await functions.create();
        await triggers.create();

        return outputErrors;
    }

    private async refreshCache() {
        const {
            updates,
            outputErrors
        } = await this.createMigrators();

        await updates.create();

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
        const columns = new ColumnsMigrator(
            this.postgres,
            this.migration,
            databaseStructure,
            outputErrors
        );
        const updates = new UpdateMigrator(
            this.postgres,
            this.migration,
            databaseStructure,
            outputErrors
        );

        return {
            functions,
            triggers,
            columns,
            updates,
            outputErrors
        };
    }
}
