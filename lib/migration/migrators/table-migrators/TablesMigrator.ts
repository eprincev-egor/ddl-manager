import { TableModel } from "../../../objects/TableModel";
import { BaseMigrator, IBaseMigratorParams } from "../base-layers/BaseMigrator";
import { TableConstraintsMigrator } from "./TableConstraintsMigrator";

import {TableCommandModel} from "../../commands/TableCommandModel";
import {RowsCommandModel} from "../../commands/RowsCommandModel";

import {UnknownTableForExtensionErrorModel} from "../../errors/UnknownTableForExtensionErrorModel";
import {CannotDropTableErrorModel} from "../../errors/CannotDropTableErrorModel";

import { MigrationModel } from "../../MigrationModel";
import { NameValidator } from "../validators/NameValidator";
import { TableValuesValidator } from "../validators/TableValuesValidator";
import { ColumnsMigrator } from "./ColumnsMigrator";

export class TablesMigrator
extends BaseMigrator<TableModel> {
    private nameValidator: NameValidator;
    private tableValuesValidator: TableValuesValidator;

    private constraintsMigrator: TableConstraintsMigrator;
    private columnsMigrator: ColumnsMigrator;

    constructor(params: IBaseMigratorParams) {
        super(params);

        this.constraintsMigrator = new TableConstraintsMigrator(params);
        this.columnsMigrator = new ColumnsMigrator(params);

        this.nameValidator = new NameValidator(params);
        this.tableValuesValidator = new TableValuesValidator(params);
    }

    protected calcChanges() {
        return this.fs.compareTablesWithDB(this.db);
    }

    migrate(migration: MigrationModel) {
        super.migrate(migration);
        
        this.fs.row.extensions.each((fsExtensionModel) => {
            const tableIdentify = fsExtensionModel.get("forTableIdentify");
            const fsTableModel = this.fs.row.tables.getByIdentify(tableIdentify);

            if ( !fsTableModel ) {
                const errorModel = new UnknownTableForExtensionErrorModel({
                    filePath: fsExtensionModel.get("filePath"),
                    tableIdentify,
                    extensionName: fsExtensionModel.get("name")
                });

                this.migration.addError(errorModel);
                return;
            }
        });
    }

    protected onCreate(fsTableModel: TableModel) {
        if ( fsTableModel.get("deprecated") ) {
            return;
        }

        const isValidTable = this.validateTable(fsTableModel);

        if ( isValidTable ) {
            this.createTable(fsTableModel);
        }
    }

    protected onChange(prev: TableModel, next: TableModel) {
        const fsTableModel = next;
        const dbTableModel = prev;

        if ( fsTableModel.get("deprecated") ) {
            return;
        }

        const isValidTable = this.validateTable(fsTableModel);

        if ( isValidTable ) {
            this.migrateTable(
                fsTableModel,
                dbTableModel
            );
        }
    }

    protected onRemove(dbTableModel: TableModel) {
        if ( this.mode === "dev" ) {
            const errorModel = new CannotDropTableErrorModel({
                filePath: "(database)",
                tableIdentify: dbTableModel.getIdentify()
            });
            this.migration.addError(errorModel);
        }
    }

    migrateTable(
        fsTableModel: TableModel,
        dbTableModel: TableModel
    ) {
        const fsTableIdentify = fsTableModel.get("identify");
        const extensions = this.fs.findExtensionsForTable( fsTableIdentify );
        const fullFSTableModel = fsTableModel.concatWithExtensions(extensions);
        
        this.columnsMigrator.migrate(
            this.migration,
            fullFSTableModel,
            dbTableModel
        );

        this.constraintsMigrator.migrate(this.migration,
            fullFSTableModel,
            dbTableModel
        );

        this.createTableValues(fullFSTableModel);
    }

    validateTable(tableModel: TableModel): boolean {
        const invalidName = this.nameValidator.validate(tableModel);
        if ( invalidName ) {
            this.migration.addError(invalidName);
            return false;
        }
        
        const invalidValues = this.tableValuesValidator.validate(tableModel);
        if ( invalidValues ) {
            this.migration.addError(invalidValues);
            return false;
        }

        return true;
    }

    createTable(fsTableModel: TableModel) {
        const createTableCommand = new TableCommandModel({
            type: "create",
            table: fsTableModel
        });
        this.migration.addCommand(createTableCommand);

        this.createTableValues(fsTableModel);
    }

    createTableValues(fsTableModel: TableModel) {
        // recreate table rows
        if ( !fsTableModel.get("values") ) {
            return;
        }

        const createRowsCommand = new RowsCommandModel({
            type: "create",
            table: fsTableModel,
            values: fsTableModel.get("values")
        });
        this.migration.addCommand(createRowsCommand);
    }
}