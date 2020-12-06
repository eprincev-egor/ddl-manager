import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";
import { Migration } from "../Migrator/Migration";

export abstract class AbstractComparator {

    protected migration: Migration;
    protected database: Database;
    protected fs: FilesState;

    constructor(
        database: Database,
        fs: FilesState,
        migration: Migration
    ) {
        this.database = database;
        this.fs = fs;
        this.migration = migration;
    }

    abstract drop(): void;
    abstract create(): void;
}