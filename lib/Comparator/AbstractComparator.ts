import { IDatabaseDriver } from "../database/interface";
import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";
import { Migration } from "../Migrator/Migration";

export abstract class AbstractComparator {

    protected driver: IDatabaseDriver;
    protected migration: Migration;
    protected database: Database;
    protected fs: FilesState;

    constructor(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState,
        migration: Migration
    ) {
        this.driver = driver;
        this.database = database;
        this.fs = fs;
        this.migration = migration;
    }

    abstract drop(): void;
    abstract create(): void;
}