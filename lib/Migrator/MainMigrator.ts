import assert from "assert";
import { Diff } from "../Diff";
import { IDatabaseDriver } from "../database/interface";
import { FunctionsMigrator } from "./FunctionsMigrator";
import { TriggersMigrator } from "./TriggersMigrator";
import { CacheMigrator } from "./CacheMigrator";

export class MainMigrator {
    private outputErrors: Error[];
    private functions: FunctionsMigrator;
    private triggers: TriggersMigrator;
    private cache: CacheMigrator;

    static async migrate(postgres: IDatabaseDriver, diff: Diff) {
        assert.ok(diff);
        const migrator = new MainMigrator(postgres, diff);
        return await migrator.migrate();
    }
    
    private constructor(postgres: IDatabaseDriver, diff: Diff) {
        this.outputErrors = [];

        this.functions = new FunctionsMigrator(
            postgres,
            diff,
            this.outputErrors
        );
        this.triggers = new TriggersMigrator(
            postgres,
            diff,
            this.outputErrors
        );
        this.cache = new CacheMigrator(
            postgres,
            diff,
            this.outputErrors
        );
    }

    async migrate() {
        await this.triggers.drop();
        await this.functions.drop();
        await this.cache.drop();

        await this.functions.create();
        await this.triggers.create();
        await this.cache.create();

        return this.outputErrors;
    }
}
