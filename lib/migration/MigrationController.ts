import State from "../State";
import MigrationModel from "./MigrationModel";
import CommandsCollection from "./commands/CommandsCollection";
import FunctionCommandModel from "./commands/FunctionCommandModel";
import ViewCommandModel from "./commands/ViewCommandModel";
import TableCommandModel from "./commands/TableCommandModel";
import ColumnCommandModel from "./commands/ColumnCommandModel";
import TriggerCommandModel from "./commands/TriggerCommandModel";
import MigrationErrorsCollection from "./errors/MigrationErrorsCollection";
import UnknownTableForTriggerErrorModel from "./errors/UnknownTableForTriggerErrorModel";
import UnknownFunctionForTriggerErrorModel from "./errors/UnknownFunctionForTriggerErrorModel";
import MaxObjectNameSizeErrorModel from "./errors/MaxObjectNameSizeErrorModel";
import CannotDropColumnErrorModel from "./errors/CannotDropColumnErrorModel";
import CannotDropTableErrorModel from "./errors/CannotDropTableErrorModel";
import CannotChangeColumnTypeErrorModel from "./errors/CannotChangeColumnTypeErrorModel";

type TMigrationMode = "dev" | "prod";

export interface IMigrationControllerParams {
    fs: State; 
    db: State;
    mode: TMigrationMode;
}

export default class MigrationController {
    fs: State;
    db: State;
    mode: TMigrationMode;

    constructor(params: IMigrationControllerParams) {
        this.fs = params.fs;
        this.db = params.db;
        this.mode = params.mode || "prod";
    }

    generateMigration(): MigrationModel {
        const commands: CommandsCollection["TInput"] = [];
        const errors: MigrationErrorsCollection["TModel"][] = [];

        this.generateFunctions(
            commands,
            errors
        );

        this.generateViews(
            commands,
            errors
        );

        this.generateTables(
            commands,
            errors
        );

        this.generateTriggers(
            commands,
            errors
        );

        // output migration
        return new MigrationModel({
            commands,
            errors
        });
    }

    generateFunctions(
        commands: CommandsCollection["TInput"],
        errors: MigrationErrorsCollection["TModel"][]
    ) {
        // drop functions
        this.db.row.functions.each((dbFunctionModel) => {
            const dbFuncIdentify = dbFunctionModel.getIdentify();
            const fsFunctionModel = this.fs.row.functions.getByIdentify(dbFuncIdentify);

            if ( fsFunctionModel ) {
                return;
            }

            const command = new FunctionCommandModel({
                type: "drop",
                function: dbFunctionModel
            });
            commands.push( command );
        });

        // create functions
        this.fs.row.functions.each((fsFunctionModel) => {
            const fsFuncIdentify = fsFunctionModel.getIdentify();
            const dbFunctionModel = this.db.row.functions.getByIdentify(fsFuncIdentify);

            if ( dbFunctionModel ) {
                return;
            }

            const functionName = fsFunctionModel.get("name");
            if ( functionName.length > 64 ) {
                const errorModel = new MaxObjectNameSizeErrorModel({
                    filePath: fsFunctionModel.get("filePath"),
                    objectType: "function",
                    name: functionName
                });

                errors.push(errorModel);
                return;
            }

            const command = new FunctionCommandModel({
                type: "create",
                function: fsFunctionModel
            });
            commands.push( command );
        });

    }

    generateViews(
        commands: CommandsCollection["TInput"],
        errors: MigrationErrorsCollection["TModel"][]
    ) {

        // drop views
        this.db.row.views.each((dbViewModel) => {
            const dbViewIdentify = dbViewModel.getIdentify();
            const fsViewModel = this.fs.row.views.getByIdentify(dbViewIdentify);

            if ( fsViewModel ) {
                return;
            }

            const command = new ViewCommandModel({
                type: "drop",
                view: dbViewModel
            });
            commands.push( command );
        });

        // create views
        this.fs.row.views.each((fsViewModel) => {
            const fsViewIdentify = fsViewModel.getIdentify();
            const dbViewModel = this.db.row.views.getByIdentify(fsViewIdentify);

            if ( dbViewModel ) {
                return;
            }

            const viewName = fsViewModel.get("name");
            if ( viewName.length > 64 ) {
                const errorModel = new MaxObjectNameSizeErrorModel({
                    filePath: fsViewModel.get("filePath"),
                    objectType: "view",
                    name: viewName
                });

                errors.push(errorModel);
                return;
            }

            const command = new ViewCommandModel({
                type: "create",
                view: fsViewModel
            });
            commands.push(command);
        });

    }

    generateTables(
        commands: CommandsCollection["TInput"],
        errors: MigrationErrorsCollection["TModel"][]
    ) {

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

            if ( dbTableModel ) {
                // create columns
                const dbColumns = dbTableModel.get("columns");
                const fsColumns = fsTableModel.get("columns");

                fsColumns.forEach((fsColumnModel) => {
                    const key = fsColumnModel.get("key");
                    const existsDbColumn = dbColumns.find((dbColumn) =>
                        dbColumn.get("key") === key
                    );

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

                return;
            }

            const createTableCommand = new TableCommandModel({
                type: "create",
                table: fsTableModel
            });
            commands.push(createTableCommand);
        });

        // error on drop columns
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

    generateTriggers(
        commands: CommandsCollection["TInput"],
        errors: MigrationErrorsCollection["TModel"][]
    ) {

        // drop trigger
        this.db.row.triggers.each((dbTriggerModel) => {
            const dbTriggerIdentify = dbTriggerModel.getIdentify();
            const fsTriggerModel = this.fs.row.triggers.getByIdentify(dbTriggerIdentify);

            if ( fsTriggerModel ) {
                return;
            }

            const command = new TriggerCommandModel({
                type: "drop",
                trigger: dbTriggerModel
            });
            commands.push( command );
        });

        // create trigger
        this.fs.row.triggers.each((fsTriggerModel) => {
            const fsTriggerIdentify = fsTriggerModel.getIdentify();
            const dbTriggerModel = this.db.row.triggers.getByIdentify(fsTriggerIdentify);

            if ( dbTriggerModel ) {
                return;
            }

            const triggerName = fsTriggerModel.get("name");
            if ( triggerName.length > 64 ) {
                const errorModel = new MaxObjectNameSizeErrorModel({
                    filePath: fsTriggerModel.get("filePath"),
                    objectType: "trigger",
                    name: triggerName
                });

                errors.push(errorModel);
                return;
            }

            const functionIdentify = fsTriggerModel.get("functionIdentify");
            const fsFunctionModel = this.fs.row.functions.getByIdentify(functionIdentify);
            if ( !fsFunctionModel ) {
                const errorModel = new UnknownFunctionForTriggerErrorModel({
                    filePath: fsTriggerModel.get("filePath"),
                    functionIdentify,
                    triggerName: fsTriggerModel.get("name")
                });

                errors.push(errorModel);
                return;
            }

            const tableIdentify = fsTriggerModel.get("tableIdentify");
            const fsTableModel = this.fs.row.tables.getByIdentify(tableIdentify);

            if ( !fsTableModel ) {
                const errorModel = new UnknownTableForTriggerErrorModel({
                    filePath: fsTriggerModel.get("filePath"),
                    tableIdentify,
                    triggerName: fsTriggerModel.get("name")
                });

                errors.push(errorModel);
                return;
            }

            const command = new TriggerCommandModel({
                type: "create",
                trigger: fsTriggerModel
            });
            commands.push(command);
        });

    }
}