import { TableModel } from "../../objects/TableModel";
import { BaseMigrator, IBaseMigratorParams } from "./base-layers/BaseMigrator";
import { TableConstraintsMigrator } from "./TableConstraintsMigrator";

import {TableCommandModel} from "../commands/TableCommandModel";
import {ColumnCommandModel} from "../commands/ColumnCommandModel";
import {RowsCommandModel} from "../commands/RowsCommandModel";
import {ColumnNotNullCommandModel} from "../commands/ColumnNotNullCommandModel";

import {UnknownTableForExtensionErrorModel} from "../errors/UnknownTableForExtensionErrorModel";
import {CannotDropColumnErrorModel} from "../errors/CannotDropColumnErrorModel";
import {CannotDropTableErrorModel} from "../errors/CannotDropTableErrorModel";
import {CannotChangeColumnTypeErrorModel} from "../errors/CannotChangeColumnTypeErrorModel";
import {ExpectedPrimaryKeyForRowsErrorModel} from "../errors/ExpectedPrimaryKeyForRowsErrorModel";

import { MigrationModel } from "../MigrationModel";
import { NameValidator } from "./validators/NameValidator";

export class TablesMigrator
extends BaseMigrator<TableModel> {
    private nameValidator: NameValidator;
    private constraintsMigrator: TableConstraintsMigrator;

    constructor(params: IBaseMigratorParams) {
        super(params);

        this.constraintsMigrator = new TableConstraintsMigrator(params);

        this.nameValidator = new NameValidator(params);
    }

    protected calcChanges() {
        const fsTables = this.fs.row.tables;
        const dbTables = this.db.row.tables;
        const changes = fsTables.compareWithDB(dbTables);
        return changes;
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

        const invalidName = this.nameValidator.validate(fsTableModel);
        if ( invalidName ) {
            this.migration.addError(invalidName);
            return;
        }

        const isValidTableValues = this.validateTableValues(fsTableModel);
        if ( !isValidTableValues ) {
            return;
        }

        this.createTable(fsTableModel);
    }

    protected onChange(prev: TableModel, next: TableModel) {
        const fsTableModel = next;
        const dbTableModel = prev;

        if ( fsTableModel.get("deprecated") ) {
            return;
        }

        const invalidName = this.nameValidator.validate(fsTableModel);
        if ( invalidName ) {
            this.migration.addError(invalidName);
            return;
        }

        const isValidTableValues = this.validateTableValues(fsTableModel);
        if ( !isValidTableValues ) {
            return;
        }

        this.generateTableMigration(
            fsTableModel,
            dbTableModel
        );
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


    private validateTableValues(tableModel: TableModel): boolean {
        const hasValues = !!tableModel.get("values");
        const hasPrimaryKey = !!tableModel.get("primaryKey");

        if ( hasValues && !hasPrimaryKey ) {
            const errorModel = new ExpectedPrimaryKeyForRowsErrorModel({
                filePath: tableModel.get("filePath"),
                tableIdentify: tableModel.getIdentify()
            });

            this.migration.addError(errorModel);

            return false;
        }

        return true;
    }

    generateTableMigration(
        fsTableModel: TableModel,
        dbTableModel: TableModel
    ) {
        const fsTableIdentify = fsTableModel.get("identify");

        // create columns
        const dbColumns = dbTableModel.get("columns");

        const fsColumns = fsTableModel.get("columns").slice();
        const extensions = this.fs.findExtensionsForTable( fsTableIdentify );
        extensions.forEach(extension => {
            extension.get("columns").forEach(column => {
                fsColumns.push(column);
            });
        });

        fsColumns.forEach((fsColumnModel) => {
            const key = fsColumnModel.get("key");
            const existsDbColumn = dbTableModel.getColumnByKey(key);

            if ( existsDbColumn ) {
                const newType = fsColumnModel.get("type");
                const oldType = existsDbColumn.get("type");

                if ( newType !== oldType ) {
                    const errorModel = new CannotChangeColumnTypeErrorModel({
                        filePath: fsTableModel.get("filePath"),
                        tableIdentify: fsTableIdentify,
                        columnKey: key,
                        oldType,
                        newType
                    });
                    this.migration.addError(errorModel);
                }

                const fsNulls = fsColumnModel.get("nulls");
                const dbNulls = existsDbColumn.get("nulls")
                if ( fsNulls !== dbNulls ) {
                    const isDrop = (
                        fsNulls === true && 
                        dbNulls === false
                    );

                    const notNullCommand = new ColumnNotNullCommandModel({
                        type: isDrop ? "drop" : "create",
                        tableIdentify: fsTableIdentify,
                        columnIdentify: fsColumnModel.get("identify")
                    });
                    this.migration.addCommand(notNullCommand);
                }

                return;
            }
            
            const createColumnCommand = new ColumnCommandModel({
                type: "create",
                tableIdentify: dbTableModel.get("identify"),
                column: fsColumnModel
            });
            this.migration.addCommand(createColumnCommand);
        });

        // dropped columns
        if ( this.mode === "dev" ) {
            dbColumns.forEach((dbColumnModel) => {
                const key = dbColumnModel.get("key");
                const existsFsColumn = fsColumns.find((fsColumn) =>
                    fsColumn.get("key") === key
                );

                if ( existsFsColumn ) {
                    return;
                }

                const isDeprecatedColumn = fsTableModel.row.deprecatedColumns.includes(key);
                if ( isDeprecatedColumn ) {
                    return;
                }

                const errorModel = new CannotDropColumnErrorModel({
                    filePath: fsTableModel.get("filePath"),
                    tableIdentify: fsTableIdentify,
                    columnKey: key
                });
                this.migration.addError(errorModel);
            });
        }

        this.constraintsMigrator.migrate(this.migration,
            fsTableModel,
            dbTableModel
        );

        this.createTableValues(fsTableModel);
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