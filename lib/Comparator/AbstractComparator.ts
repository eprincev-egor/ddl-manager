import { IDatabaseDriver } from "../database/interface";
import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";
import { Migration } from "../Migrator/Migration";

export abstract class AbstractComparator {

    constructor(
        protected driver: IDatabaseDriver,
        protected database: Database,
        protected fs: FilesState,
        protected migration: Migration
    ) {}

    abstract drop(): void;
    abstract create(): void;
}