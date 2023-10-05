import { Migration } from "./Migration";
import { IDatabaseDriver } from "../database/interface";
import { FunctionsMigrator } from "./FunctionsMigrator";
import { TriggersMigrator } from "./TriggersMigrator";
import { ColumnsMigrator } from "./ColumnsMigrator";
import { UpdateMigrator } from "./UpdateMigrator";
import { Database } from "../database/schema/Database";
import { IndexesMigrator } from "./IndexesMigrator";

export class MainMigrator {
    static async migrate(
        postgres: IDatabaseDriver,
        database: Database,
        migration: Migration
    ) {
        const migrator = new MainMigrator(
            postgres,
            database,
            migration
        );
        return await migrator.migrate();
    }

    constructor(
        private postgres: IDatabaseDriver,
        private database: Database,
        private migration: Migration,
        private outputErrors: Error[] = [],
        private aborted = false
    ) {}

    private functions? = new FunctionsMigrator(
        this.postgres,
        this.migration,
        this.database,
        this.outputErrors
    );
    private triggers? = new TriggersMigrator(
        this.postgres,
        this.migration,
        this.database,
        this.outputErrors
    );
    private columns? = new ColumnsMigrator(
        this.postgres,
        this.migration,
        this.database,
        this.outputErrors
    );
    private updates? = new UpdateMigrator(
        this.postgres,
        this.migration,
        this.database,
        this.outputErrors
    );
    private indexes? = new IndexesMigrator(
        this.postgres,
        this.migration,
        this.database,
        this.outputErrors
    );

    async migrate() {
        await this.triggers?.drop();
        await this.functions?.drop();
        await this.indexes?.drop();
        await this.columns?.drop();

        await this.columns?.create();
        await this.functions?.create();
        await this.triggers?.create();
        await this.indexes?.create();

        await this.updates?.create();

        return this.outputErrors;
    }

    abort() {
        if ( this.isAborted() ) {
            return;
        }
        this.aborted = true;
        this.updates?.abort();
        this.triggers = undefined;
        this.functions = undefined;
        this.columns = undefined;
        this.indexes = undefined;
        this.updates = undefined;
    }

    isAborted() {
        return this.aborted;
    }
}
