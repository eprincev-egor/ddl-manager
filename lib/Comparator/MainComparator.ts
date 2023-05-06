import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";
import { Migration } from "../Migrator/Migration";
import { CacheComparator } from "./CacheComparator";
import { TriggersComparator } from "./TriggersComparator";
import { FunctionsComparator } from "./FunctionsComparator";
import { IndexComparator } from "./IndexComparator";
import { IDatabaseDriver } from "../database/interface";

export class MainComparator {

    static async compare(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState
    ) {
        const comparator = new MainComparator(driver, database, fs);
        return await comparator.compare();
    }

    static async logAllFuncsMigration(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState
    ) {
        const comparator = new MainComparator(driver, database, fs);
        return await comparator.logAllFuncsMigration();
    }

    static async compareWithoutUpdates(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState
    ) {
        const comparator = new MainComparator(driver, database, fs);
        return await comparator.compareWithoutUpdates();
    }

    static async refreshCache(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState,
        concreteTables?: string
    ) {
        const comparator = new MainComparator(driver, database, fs);
        return await comparator.refreshCache(concreteTables);
    }

    private migration: Migration;
    private functions: FunctionsComparator;
    private triggers: TriggersComparator;
    private cache: CacheComparator;
    private indexes: IndexComparator;

    private constructor(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState
    ) {
        this.migration = Migration.empty();

        this.functions = new FunctionsComparator(
            driver,
            database,
            fs,
            this.migration
        );
        this.triggers = new TriggersComparator(
            driver,
            database,
            fs,
            this.migration
        );
        this.cache = new CacheComparator(
            driver,
            database,
            fs,
            this.migration
        );
        this.indexes = new IndexComparator(
            driver,
            database,
            fs,
            this.migration
        );
    }

    private async compare() {
        // need add new functions to migration
        // before rebuild cache
        this.functions.create();

        await this.dropOldObjects();

        this.triggers.create();
        await this.cache.create();
        this.indexes.create();

        return this.migration;
    }

    private async logAllFuncsMigration() {
        this.functions.createLogFuncs();
        await this.cache.createLogFuncs();

        return this.migration;
    }

    private async compareWithoutUpdates() {
        // need add new functions to migration
        // before rebuild cache
        this.functions.create();

        await this.dropOldObjects();

        this.triggers.create();
        await this.cache.createWithoutUpdates();
        this.indexes.create();

        return this.migration;
    }

    private async refreshCache(concreteTables?: string) {
        await this.cache.refreshCache(concreteTables);
        return this.migration;
    }

    private async dropOldObjects() {
        this.functions.drop();
        this.triggers.drop();
        await this.cache.drop();
        this.indexes.drop();
    }
}
