import { AbstractMigrator } from "../AbstractMigrator";
import { CacheTriggersMigrator } from "./CacheTriggersMigrator";
import { Diff } from "../../Diff";
import { Database as DatabaseStructure } from "../../database/schema/Database";
import { IDatabaseDriver } from "../../database/interface";
import { CacheColumnsMigrator } from "./CacheColumnsMigrator";

export class MainCacheMigrator extends AbstractMigrator {

    private triggers: CacheTriggersMigrator;
    private columns: CacheColumnsMigrator;

    constructor(
        postgres: IDatabaseDriver,
        diff: Diff,
        databaseStructure: DatabaseStructure,
        outputErrors: Error[]
    ) {
        super(postgres, diff, databaseStructure, outputErrors);

        this.triggers = new CacheTriggersMigrator(
            postgres,
            diff,
            databaseStructure,
            outputErrors
        );
        this.columns = new CacheColumnsMigrator(
            postgres,
            diff,
            databaseStructure,
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
    }

    async createWithoutUpdateCacheColumns() {
        await this.columns.createWithoutUpdateCacheColumns();
        await this.triggers.create();
    }
}
