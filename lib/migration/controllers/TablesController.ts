import {BaseValidationsController} from "./base-layers/BaseValidationsController";
import {IMigrationControllerParams} from "../IMigrationControllerParams";
import {TableConstraintController} from "./TableConstraintController";
import {TableModel} from "../../objects/TableModel";

import {TableCommandModel} from "../commands/TableCommandModel";
import {ColumnCommandModel} from "../commands/ColumnCommandModel";
import {RowsCommandModel} from "../commands/RowsCommandModel";
import {ColumnNotNullCommandModel} from "../commands/ColumnNotNullCommandModel";

import {UnknownTableForExtensionErrorModel} from "../errors/UnknownTableForExtensionErrorModel";
import {CannotDropColumnErrorModel} from "../errors/CannotDropColumnErrorModel";
import {CannotDropTableErrorModel} from "../errors/CannotDropTableErrorModel";
import {CannotChangeColumnTypeErrorModel} from "../errors/CannotChangeColumnTypeErrorModel";
import {ExpectedPrimaryKeyForRowsErrorModel} from "../errors/ExpectedPrimaryKeyForRowsErrorModel";


export class TablesController 
extends BaseValidationsController {
    constraintController: TableConstraintController;

    constructor(params: IMigrationControllerParams) {
        super(params);

        this.constraintController = new TableConstraintController(params);
    }

    generate() {

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

        // create tables
        const dbTables = this.db.row.tables;
        const fsTables = this.fs.row.tables;
        const changes = fsTables.compareWithDB(dbTables);

        changes.created.forEach((fsTableModel) => {
            if ( fsTableModel.get("deprecated") ) {
                return;
            }

            if ( !fsTableModel.isValidNameLength() ) {
                const errorModel = this.createInvalidNameError(fsTableModel);
                this.migration.addError(errorModel);
                return;
            }

            const isValidTableValues = this.validateTableValues(fsTableModel);
            if ( !isValidTableValues ) {
                return;
            }

            const createTableCommand = new TableCommandModel({
                type: "create",
                table: fsTableModel
            });
            this.migration.addCommand(createTableCommand);

            if ( fsTableModel.get("values") ) {
                const createRowsCommand = new RowsCommandModel({
                    type: "create",
                    table: fsTableModel,
                    values: fsTableModel.get("values")
                });
                this.migration.addCommand(createRowsCommand);
            }
        });

        
        changes.changed.forEach(({prev, next}) => {
            const fsTableModel = next;
            const dbTableModel = prev;

            if ( fsTableModel.get("deprecated") ) {
                return;
            }

            if ( !fsTableModel.isValidNameLength() ) {
                const errorModel = this.createInvalidNameError(fsTableModel);
                this.migration.addError(errorModel);
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
        });
        

        // error on drop table
        if ( this.mode === "dev" ) {
            changes.removed.forEach((dbTableModel) => {
                const errorModel = new CannotDropTableErrorModel({
                    filePath: "(database)",
                    tableIdentify: dbTableModel.getIdentify()
                });
                this.migration.addError(errorModel);
            });
        }
    }

    validateTableValues(tableModel: TableModel): boolean {
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

        this.constraintController.setMigration(this.migration);
        this.constraintController.generateConstraintMigration(
            fsTableModel,
            dbTableModel
        );

        
        // recreate table rows
        if ( fsTableModel.get("values") ) {
            const createRowsCommand = new RowsCommandModel({
                type: "create",
                table: fsTableModel,
                values: fsTableModel.get("values")
            });
            this.migration.addCommand(createRowsCommand);
        }
    }
}