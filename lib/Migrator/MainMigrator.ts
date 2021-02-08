import { Migration } from "./Migration";
import { IDatabaseDriver } from "../database/interface";
import { FunctionsMigrator } from "./FunctionsMigrator";
import { TriggersMigrator } from "./TriggersMigrator";
import { ColumnsMigrator } from "./ColumnsMigrator";
import { UpdateMigrator } from "./UpdateMigrator";
import { Database } from "../database/schema/Database";
import { IndexesMigrator } from "./IndexesMigrator";

export class MainMigrator {
    private migration: Migration;
    private postgres: IDatabaseDriver;
    private database: Database;

    static async migrate(
        postgres: IDatabaseDriver,
        database: Database,
        migration: Migration
    ) {
        const migrator = new MainMigrator(
            postgres,
            database,
            migration
        );
        return await migrator.migrate();
    }

    private constructor(
        postgres: IDatabaseDriver,
        database: Database,
        migration: Migration
    ) {
        this.postgres = postgres;
        this.database = database;
        this.migration = migration;
    }

    private async migrate() {
        const {
            functions,
            triggers,
            columns,
            updates,
            indexes,
            outputErrors
        } = await this.createMigrators();

        await triggers.drop();
        await functions.drop();
        await columns.drop();
        await indexes.drop();

        await columns.create();
        await functions.create();
        await triggers.create();
        await indexes.create();
        await updates.create();

        return outputErrors;
    }

    private async createMigrators() {

        const outputErrors: Error[] = [];

        const functions = new FunctionsMigrator(
            this.postgres,
            this.migration,
            this.database,
            outputErrors
        );
        const triggers = new TriggersMigrator(
            this.postgres,
            this.migration,
            this.database,
            outputErrors
        );
        const columns = new ColumnsMigrator(
            this.postgres,
            this.migration,
            this.database,
            outputErrors
        );
        const updates = new UpdateMigrator(
            this.postgres,
            this.migration,
            this.database,
            outputErrors
        );
        const indexes = new IndexesMigrator(
            this.postgres,
            this.migration,
            this.database,
            outputErrors
        );

        return {
            functions,
            triggers,
            columns,
            updates,
            indexes,
            outputErrors
        };
    }
}
