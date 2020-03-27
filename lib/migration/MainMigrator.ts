import { MigrationModel } from "./MigrationModel";
import { FunctionsMigrator } from "./migrators/FunctionsMigrator";
import { IBaseMigratorParams } from "./migrators/base-layers/BaseMigrator";
import { ViewsMigrator } from "./migrators/ViewsMigrator";
import { TriggersMigrator } from "./migrators/TriggersMigrator";
import { TablesMigrator } from "./migrators/table-migrators/TablesMigrator";

export class MainMigrator {
    private functions: FunctionsMigrator;
    private views: ViewsMigrator;
    private triggers: TriggersMigrator;
    private tables: TablesMigrator;

    constructor(params: IBaseMigratorParams) {
        this.functions = new FunctionsMigrator(params);
        this.views = new ViewsMigrator(params);
        this.tables = new TablesMigrator(params);
        this.triggers = new TriggersMigrator(params);
    }

    migrate(): MigrationModel {
        const migration = new MigrationModel();

        this.functions.migrate(migration);
        this.views.migrate(migration);
        this.tables.migrate(migration);
        this.triggers.migrate(migration);

        return migration;
    }
}