import {PrimaryKeyCommandModel} from "../../commands/PrimaryKeyCommandModel";
import {TableModel} from "../../../objects/TableModel";
import { IBaseMigratorParams } from "../base-layers/BaseMigrator";
import { MigrationModel } from "../../MigrationModel";
import { FSDDLState } from "../../../state/FSDDLState";
import { DDLState } from "../../../state/DDLState";

export class PrimaryKeyMigrator {
    protected migration: MigrationModel;
    protected fs: FSDDLState;
    protected db: DDLState;
    private fsTableModel: TableModel;
    private dbTableModel: TableModel;

    constructor(params: IBaseMigratorParams) {
        this.fs = params.fs;
        this.db = params.db;
    }

    migrate(
        migration: MigrationModel,
        fsTableModel: TableModel,
        dbTableModel: TableModel
    ) {
        this.migration = migration;
        this.fsTableModel = fsTableModel;
        this.dbTableModel = dbTableModel;
        
        // create/drop primary key
        this.migratePrimaryKey();

    }

    private migratePrimaryKey() {
        const tableIdentify = this.fsTableModel.getIdentify();

        const fsPrimaryKey = this.fsTableModel.get("primaryKey");
        const dbPrimaryKey = this.dbTableModel.get("primaryKey");

        if ( fsPrimaryKey && !dbPrimaryKey ) {
            const primaryKeyCommand = new PrimaryKeyCommandModel({
                type: "create",
                tableIdentify,
                primaryKey: fsPrimaryKey
            });
            this.migration.addCommand(primaryKeyCommand);
        }

        if ( !fsPrimaryKey && dbPrimaryKey ) {
            const primaryKeyCommand = new PrimaryKeyCommandModel({
                type: "drop",
                tableIdentify,
                primaryKey: dbPrimaryKey
            });
            this.migration.addCommand(primaryKeyCommand);
        }

        if ( fsPrimaryKey && dbPrimaryKey ) {
            const isEqual = (
                fsPrimaryKey.length === dbPrimaryKey.length &&
                fsPrimaryKey.every(key => dbPrimaryKey.includes(key))
            );

            if ( !isEqual ) {
                const dropPrimaryKeyCommand = new PrimaryKeyCommandModel({
                    type: "drop",
                    tableIdentify,
                    primaryKey: dbPrimaryKey
                });
                this.migration.addCommand(dropPrimaryKeyCommand);

                const createPrimaryKeyCommand = new PrimaryKeyCommandModel({
                    type: "create",
                    tableIdentify,
                    primaryKey: fsPrimaryKey
                });
                this.migration.addCommand(createPrimaryKeyCommand);
            }
        }

    }
}