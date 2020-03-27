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
        const fsPrimaryKey = this.fsTableModel.get("primaryKey");
        const dbPrimaryKey = this.dbTableModel.get("primaryKey");

        const isCreate = (
            fsPrimaryKey && 
            !dbPrimaryKey
        );
        const isDrop = (
            !fsPrimaryKey && 
            dbPrimaryKey
        );
        const isChange = (
            fsPrimaryKey && 
            dbPrimaryKey &&
            !equalArrays(fsPrimaryKey, dbPrimaryKey)
        );

        if ( isCreate ) {
            this.create(fsPrimaryKey);
        }

        if ( isDrop ) {
            this.drop(dbPrimaryKey);
        }

        if ( isChange ) {
            this.drop(dbPrimaryKey);
            this.create(fsPrimaryKey);
        }
    }

    private drop(primaryKey: string[]) {
        const tableIdentify = this.fsTableModel.getIdentify();
        const primaryKeyCommand = new PrimaryKeyCommandModel({
            type: "drop",
            tableIdentify,
            primaryKey
        });
        this.migration.addCommand(primaryKeyCommand);
    }

    private create(primaryKey: string[]) {
        const tableIdentify = this.fsTableModel.getIdentify();
        const createPrimaryKeyCommand = new PrimaryKeyCommandModel({
            type: "create",
            tableIdentify,
            primaryKey
        });
        this.migration.addCommand(createPrimaryKeyCommand);
    }
}

function equalArrays(arr1: string[], arr2: string[]): boolean {
    return (
        arr1.length === arr2.length &&
        arr1.every(key => arr2.includes(key))
    );
}