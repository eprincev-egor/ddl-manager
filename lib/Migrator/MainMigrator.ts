import assert from "assert";
import { Diff } from "../Diff";
import { IDatabaseDriver } from "../database/interface";
import { FunctionsMigrator } from "./FunctionsMigrator";
import { TriggersMigrator } from "./TriggersMigrator";
import { MainCacheMigrator } from "./cache/MainCacheMigrator";

export class MainMigrator {
    private diff: Diff;
    private postgres: IDatabaseDriver;

    static async migrate(postgres: IDatabaseDriver, diff: Diff) {
        assert.ok(diff);
        const migrator = new MainMigrator(postgres, diff);
        return await migrator.migrate();
    }
    
    private constructor(postgres: IDatabaseDriver, diff: Diff) {
        this.postgres = postgres;
        this.diff = diff;
    }

    async migrate() {
        const databaseStructure = await this.postgres.loadTables();
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

        await triggers.drop();
        await functions.drop();
        await cache.drop();

        await functions.create();
        await triggers.create();
        await cache.create();

        return outputErrors;
    }
}
