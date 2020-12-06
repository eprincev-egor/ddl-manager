import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";
import { Migration } from "../Migrator/Migration";
import { FSEvent } from "../fs/FSEvent";
import { CacheComparator } from "./CacheComparator";
import { TriggersComparator } from "./TriggersComparator";
import { FunctionsComparator } from "./FunctionsComparator";

export class MainComparator {

    static compare(database: Database, fs: FilesState) {
        const comparator = new MainComparator(database, fs);
        return comparator.compare();
    }

    static fsEventToMigration(database: Database, fs: FilesState, fsEvent: FSEvent) {
        const comparator = new MainComparator(database, fs);
        return comparator.fsEventToMigration(fsEvent);
    }

    private database: Database;
    private migration: Migration;
    private functions: FunctionsComparator;
    private triggers: TriggersComparator;
    private cache: CacheComparator;

    private constructor(database: Database, fs: FilesState) {
        this.database = database;
        this.migration = Migration.empty();

        this.functions = new FunctionsComparator(
            database,
            fs,
            this.migration
        );
        this.triggers = new TriggersComparator(
            database,
            fs,
            this.migration
        );
        this.cache = new CacheComparator(
            database,
            fs,
            this.migration
        );
    }

    private compare() {
        this.dropOldObjects();
        this.createNewObjects();

        return this.migration;
    }

    fsEventToMigration(fsEvent: FSEvent) {
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
            this.database,
            tmpFs,
            this.migration
        );
        cacheComparator.createWithoutUpdates();

        return this.migration;
    }

    private dropOldObjects() {
        this.functions.drop();
        this.triggers.drop();
        this.cache.drop();
    }

    private createNewObjects() {
        this.functions.create();
        this.triggers.create();
        this.cache.create();
    }

}
