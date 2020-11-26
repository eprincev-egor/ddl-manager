import { AbstractMigrator } from "../AbstractMigrator";
import { CacheTriggersMigrator } from "./CacheTriggersMigrator";
import { Diff } from "../../Diff";
import { IDatabaseDriver } from "../../database/interface";
import { CacheColumnsMigrator } from "./CacheColumnsMigrator";

export class MainCacheMigrator extends AbstractMigrator {

    private triggers: CacheTriggersMigrator;
    private columns: CacheColumnsMigrator;

    constructor(
        postgres: IDatabaseDriver,
        diff: Diff,
        outputErrors: Error[]
    ) {
        super(postgres, diff, outputErrors);

        this.triggers = new CacheTriggersMigrator(
            postgres,
            diff,
            outputErrors
        );
        this.columns = new CacheColumnsMigrator(
            postgres,
            diff,
            outputErrors
        );
    }

    async drop() {
        await this.triggers.drop();
        await this.columns.drop();
    }

    async create() {
        await this.columns.create();
        await this.triggers.create();

        await this.postgres.saveCacheMeta(this.diff.create.cache);
    }

}
