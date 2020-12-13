import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";
import { Migration } from "../Migrator/Migration";
import { FSEvent } from "../fs/FSEvent";
import { CacheComparator } from "./CacheComparator";
import { TriggersComparator } from "./TriggersComparator";
import { FunctionsComparator } from "./FunctionsComparator";
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

    static async fsEventToMigration(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState,
        fsEvent: FSEvent
    ) {
        const comparator = new MainComparator(driver, database, fs);
        return await comparator.fsEventToMigration(fsEvent);
    }

    private driver: IDatabaseDriver;
    private database: Database;
    private migration: Migration;
    private functions: FunctionsComparator;
    private triggers: TriggersComparator;
    private cache: CacheComparator;

    private constructor(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState
    ) {
        this.driver = driver;
        this.database = database;
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
    }

    private async compare() {
        await this.dropOldObjects();
        await this.createNewObjects();

        return this.migration;
    }

    async fsEventToMigration(fsEvent: FSEvent) {
        for (const removedFile of fsEvent.removed) {
            this.migration.drop({
                functions: removedFile.content.functions,
                triggers: removedFile.content.triggers
            });
        }

        for (const createdFile of fsEvent.created) {
            this.migration.create({
                functions: createdFile.content.functions,
                triggers: createdFile.content.triggers
            });
        }

        // TODO: drop columns, triggers

        const tmpFs = new FilesState();
        for (const file of fsEvent.created) {
            tmpFs.addFile(file);
        }

        const cacheComparator = new CacheComparator(
            this.driver,
            this.database,
            tmpFs,
            this.migration
        );
        await cacheComparator.createWithoutUpdates();

        return this.migration;
    }

    private async dropOldObjects() {
        this.functions.drop();
        this.triggers.drop();
        await this.cache.drop();
    }

    private async createNewObjects() {
        this.functions.create();
        this.triggers.create();
        await this.cache.create();
    }

}
