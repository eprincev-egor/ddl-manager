import {TableModel} from "../../../objects/TableModel";
import { IBaseMigratorParams } from "../base-layers/BaseMigrator";
import { MigrationModel } from "../../MigrationModel";
import { PrimaryKeyMigrator } from "./PrimaryKeyMigrator";
import { CheckConstraintsMigrator } from "./CheckConstraintsMigrator";
import { UniqueConstraintsMigrator } from "./UniqueConstraintsMigrator";
import { ForeignKeyConstraintsMigrator } from "./ForeignKeyConstraintsMigrator";

export class TableConstraintsMigrator {
    private primaryKey: PrimaryKeyMigrator;
    private checkConstraints: CheckConstraintsMigrator;
    private uniqueConstraints: UniqueConstraintsMigrator;
    private foreignKeysConstraints: ForeignKeyConstraintsMigrator;

    constructor(params: IBaseMigratorParams) {
        this.primaryKey = new PrimaryKeyMigrator(params);
        this.checkConstraints = new CheckConstraintsMigrator(params);
        this.uniqueConstraints = new UniqueConstraintsMigrator(params);
        this.foreignKeysConstraints = new ForeignKeyConstraintsMigrator(params);
    }

    migrate(
        migration: MigrationModel,
        fsTableModel: TableModel,
        dbTableModel: TableModel
    ) {
        // create/drop primary key
        this.primaryKey.migrate(
            migration,
            fsTableModel,
            dbTableModel
        );

        // create/drop check constraints
        this.checkConstraints.migrate(
            migration,
            fsTableModel,
            dbTableModel
        );
        
        // create/drop unique constraints
        this.uniqueConstraints.migrate(
            migration,
            fsTableModel,
            dbTableModel
        );

        // create/drop foreign key constraints
        this.foreignKeysConstraints.migrate(
            migration,
            fsTableModel,
            dbTableModel
        );
    }

}