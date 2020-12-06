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

    static fsEventToMigration(fsEvent: FSEvent) {
        const migration = Migration.empty();
        
        for (const removedFile of fsEvent.removed) {
            migration.drop({
                functions: removedFile.content.functions,
                triggers: removedFile.content.triggers
            });
        }

        for (const createdFile of fsEvent.created) {
            migration.create({
                functions: createdFile.content.functions,
                triggers: createdFile.content.triggers
            });
        }

        return migration;
    }

    private migration: Migration;
    private functions: FunctionsComparator;
    private triggers: TriggersComparator;
    private cache: CacheComparator;

    private constructor(database: Database, fs: FilesState) {
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

    compare() {
        this.dropOldObjects();
        this.createNewObjects();

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
