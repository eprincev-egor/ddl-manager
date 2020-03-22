import BaseController from "./BaseController";
import {IMigrationControllerParams} from "../IMigrationControllerParams";
import TableConstraintController from "./TableConstraintController";

import CommandsCollection from "../commands/CommandsCollection";
import MigrationErrorsCollection from "../errors/MigrationErrorsCollection";
import UnknownTableForExtensionErrorModel from "../errors/UnknownTableForExtensionErrorModel";

import TableCommandModel from "../commands/TableCommandModel";
import ColumnCommandModel from "../commands/ColumnCommandModel";

import MaxObjectNameSizeErrorModel from "../errors/MaxObjectNameSizeErrorModel";
import CannotDropColumnErrorModel from "../errors/CannotDropColumnErrorModel";
import CannotDropTableErrorModel from "../errors/CannotDropTableErrorModel";
import CannotChangeColumnTypeErrorModel from "../errors/CannotChangeColumnTypeErrorModel";
import ExpectedPrimaryKeyForRowsErrorModel from "../errors/ExpectedPrimaryKeyForRowsErrorModel";
import CreateRowsCommandModel from "../commands/RowsCommandModel";
import ColumnNotNullCommandModel from "../commands/ColumnNotNullCommandModel";
import TableModel from "../../objects/TableModel";


export default class TablesController extends BaseController {
    constraintController: TableConstraintController;

    constructor(params: IMigrationControllerParams) {
        super(params);

        this.constraintController = new TableConstraintController(params);
    }


    generate(
        commands: CommandsCollection["TInput"],
        errors: MigrationErrorsCollection["TModel"][]
    ) {

        this.fs.row.extensions.each((fsExtensionModel) => {
            const tableIdentify = fsExtensionModel.get("forTableIdentify");
            const fsTableModel = this.fs.row.tables.getByIdentify(tableIdentify);

            if ( !fsTableModel ) {
                const errorModel = new UnknownTableForExtensionErrorModel({
                    filePath: fsExtensionModel.get("filePath"),
                    tableIdentify,
                    extensionName: fsExtensionModel.get("name")
                });

                errors.push(errorModel);
                return;
            }
        });

        // create tables
        this.fs.row.tables.each((fsTableModel) => {
            if ( fsTableModel.get("deprecated") ) {
                return;
            }

            const fsTableIdentify = fsTableModel.getIdentify();
            const dbTableModel = this.db.row.tables.getByIdentify(fsTableIdentify);

            const tableName = fsTableModel.get("name");
            if ( tableName.length > 64 ) {
                const errorModel = new MaxObjectNameSizeErrorModel({
                    filePath: fsTableModel.get("filePath"),
                    objectType: "table",
                    name: tableName
                });

                errors.push(errorModel);
                return;
            }

            if ( fsTableModel.get("values") ) {
                const primaryKey = fsTableModel.get("primaryKey");
                if ( !primaryKey ) {
                    const errorModel = new ExpectedPrimaryKeyForRowsErrorModel({
                        filePath: fsTableModel.get("filePath"),
                        tableIdentify: fsTableIdentify
                    });
    
                    errors.push(errorModel);
                    return;
                }
            }

            if ( dbTableModel ) {
                this.generateTableMigration(
                    fsTableModel,
                    dbTableModel,
                    commands,
                    errors
                );
            }
            else {
                const createTableCommand = new TableCommandModel({
                    type: "create",
                    table: fsTableModel
                });
                commands.push(createTableCommand);
    
            }

            // (re)create table rows
            if ( fsTableModel.get("values") ) {
                const createRowsCommand = new CreateRowsCommandModel({
                    type: "create",
                    table: fsTableModel,
                    values: fsTableModel.get("values")
                });
                commands.push(createRowsCommand);
            }
        });

        // error on drop table
        this.db.row.tables.each((dbTableModel) => {
            const dbTableIdentify = dbTableModel.getIdentify();
            const fsTableModel = this.fs.row.tables.getByIdentify(dbTableIdentify);

            if ( fsTableModel ) {
                return;
            }

            if ( this.mode === "dev" ) {
                const errorModel = new CannotDropTableErrorModel({
                    filePath: "(database)",
                    tableIdentify: dbTableIdentify
                });
                errors.push(errorModel);
            }
        });
    }

    generateTableMigration(
        fsTableModel: TableModel,
        dbTableModel: TableModel,

        commands: CommandsCollection["TInput"],
        errors: MigrationErrorsCollection["TModel"][]
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
                    errors.push(errorModel);
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
                    commands.push(notNullCommand);
                }

                return;
            }
            
            const createColumnCommand = new ColumnCommandModel({
                type: "create",
                tableIdentify: dbTableModel.get("identify"),
                column: fsColumnModel
            });
            commands.push(createColumnCommand);
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
                errors.push(errorModel);
            });
        }

        
        this.constraintController.generateConstraintMigration(
            fsTableModel,
            dbTableModel,
            commands,
            errors
        );

    }
}