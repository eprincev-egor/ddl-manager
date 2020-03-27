import { TableModel } from "../../objects/TableModel";
import { IBaseMigratorParams } from "./base-layers/BaseMigrator";
import { MigrationModel } from "../MigrationModel";
import { ColumnModel } from "../../objects/ColumnModel";
import { CannotChangeColumnTypeErrorModel } from "../errors/CannotChangeColumnTypeErrorModel";
import { ColumnNotNullCommandModel } from "../commands/ColumnNotNullCommandModel";
import { CannotDropColumnErrorModel } from "../errors/CannotDropColumnErrorModel";
import { ColumnCommandModel } from "../commands/ColumnCommandModel";

export class ColumnsMigrator {
    protected migration: MigrationModel;
    protected mode: IBaseMigratorParams["mode"];
    private fsTableModel: TableModel;

    constructor(params: IBaseMigratorParams) {
        this.mode = params.mode;
    }

    migrate(
        migration: MigrationModel,
        fsTableModel: TableModel,
        dbTableModel: TableModel
    ) {
        this.migration = migration;
        this.fsTableModel = fsTableModel;
        
        const {
            created,
            removed,
            changed
        } = fsTableModel.compareColumnsWithDBTable(dbTableModel);
        
        removed.forEach((column) => {
            this.onRemove(column);
        });

        changed.forEach(({prev, next}) => {
            this.onChange(prev, next);
        });

        created.forEach((column) => {
            this.onCreate(column);
        });
    }

    onRemove(column: ColumnModel) {
        if ( this.mode === "dev" ) {
            const key = column.get("key");
            const isDeprecatedColumn = this.fsTableModel.isDeprecatedColumn(key);
            if ( isDeprecatedColumn ) {
                return;
            }

            const errorModel = new CannotDropColumnErrorModel({
                filePath: this.fsTableModel.get("filePath"),
                tableIdentify: this.fsTableModel.getIdentify(),
                columnKey: key
            });
            this.migration.addError(errorModel);
        }
    }

    onChange(dbColumn: ColumnModel, fsColumn: ColumnModel) {
        const tableIdentify = this.fsTableModel.getIdentify();
        const newType = fsColumn.get("type");
        const oldType = dbColumn.get("type");

        if ( newType !== oldType ) {
            const errorModel = new CannotChangeColumnTypeErrorModel({
                filePath: this.fsTableModel.get("filePath"),
                tableIdentify,
                columnKey: fsColumn.get("key"),
                oldType,
                newType
            });
            this.migration.addError(errorModel);
        }

        const fsNulls = fsColumn.get("nulls");
        const dbNulls = dbColumn.get("nulls")
        if ( fsNulls !== dbNulls ) {
            const isDrop = (
                fsNulls === true && 
                dbNulls === false
            );

            const notNullCommand = new ColumnNotNullCommandModel({
                type: isDrop ? "drop" : "create",
                tableIdentify,
                columnIdentify: fsColumn.get("identify")
            });
            this.migration.addCommand(notNullCommand);
        }

    }

    onCreate(column: ColumnModel) {
        const tableIdentify = this.fsTableModel.getIdentify();

        const createColumnCommand = new ColumnCommandModel({
            type: "create",
            tableIdentify,
            column
        });
        this.migration.addCommand(createColumnCommand);
    }
}