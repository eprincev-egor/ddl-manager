import {TableModel} from "../../../objects/TableModel";
import { MigrationModel } from "../../MigrationModel";
import { SimpleMigrator, IAllowedToDrop } from "./SimpleMigrator";
import { BaseDBObjectModel } from "../../../objects/base-layers/BaseDBObjectModel";

export abstract class ConstraintMigrator<ConstraintModel extends BaseDBObjectModel<any> & IAllowedToDrop>
extends SimpleMigrator<ConstraintModel> {
    protected fsTableModel: TableModel;
    protected dbTableModel: TableModel;

    migrateTable(
        migration: MigrationModel,
        fsTableModel: TableModel,
        dbTableModel: TableModel
    ) {
        this.fsTableModel = fsTableModel;
        this.dbTableModel = dbTableModel;

        super.migrate(migration);
    }
}