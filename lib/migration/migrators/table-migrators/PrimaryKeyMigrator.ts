import { PrimaryKeyCommandModel } from "../../commands/PrimaryKeyCommandModel";
import { TableModel} from "../../../objects/TableModel";
import { MigrationModel } from "../../MigrationModel";

export class PrimaryKeyMigrator {
    protected migration: MigrationModel;
    private fsTableModel: TableModel;
    private dbTableModel: TableModel;

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