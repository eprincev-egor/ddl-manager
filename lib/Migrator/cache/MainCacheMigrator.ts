import { AbstractMigrator } from "../AbstractMigrator";
import { CacheTriggersMigrator } from "./CacheTriggersMigrator";
import { Migration } from "../Migration";
import { Database } from "../../database/schema/Database";
import { IDatabaseDriver } from "../../database/interface";
import { CacheColumnsMigrator } from "./CacheColumnsMigrator";

export class MainCacheMigrator extends AbstractMigrator {

    private triggers: CacheTriggersMigrator;
    private columns: CacheColumnsMigrator;

    constructor(
        postgres: IDatabaseDriver,
        migration: Migration,
        database: Database,
        outputErrors: Error[]
    ) {
        super(postgres, migration, database, outputErrors);

        this.triggers = new CacheTriggersMigrator(
            postgres,
            migration,
            database,
            outputErrors
        );
        this.columns = new CacheColumnsMigrator(
            postgres,
            migration,
            database,
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
