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

export interface IMigrationOptions {
    mode: "dev" | "prod";
};
interface IMigrationControllerParams {
    fs: State; 
    db: State;
}

export default class MigrationController {
    fs: State;
    db: State;

    constructor(params: IMigrationControllerParams) {
        this.fs = params.fs;
        this.db = params.db;
    }

    generateMigration(options: IMigrationOptions): MigrationModel {
        const fs = {
            functions: this.fs.get("functions"),
            views: this.fs.get("views"),
            tables: this.fs.get("tables"),
            triggers: this.fs.get("triggers")
        };
        const db = {
            functions: this.db.get("functions"),
            views: this.db.get("views"),
            tables: this.db.get("tables"),
            triggers: this.db.get("triggers")
        };
        const commands: CommandsCollection["TInput"] = [];
        const errors: MigrationErrorsCollection["TModel"][] = [];

        // drop functions
        db.functions.each((dbFunctionModel) => {
            const dbFuncIdentify = dbFunctionModel.getIdentify();
            const fsFunctionModel = fs.functions.getByIdentify(dbFuncIdentify);

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
        fs.functions.each((fsFunctionModel) => {
            const fsFuncIdentify = fsFunctionModel.getIdentify();
            const dbFunctionModel = db.functions.getByIdentify(fsFuncIdentify);

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

        // drop views
        db.views.each((dbViewModel) => {
            const dbViewIdentify = dbViewModel.getIdentify();
            const fsViewModel = fs.views.getByIdentify(dbViewIdentify);

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
        fs.views.each((fsViewModel) => {
            const fsViewIdentify = fsViewModel.getIdentify();
            const dbViewModel = db.views.getByIdentify(fsViewIdentify);

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

        // create tables
        fs.tables.each((fsTableModel) => {
            if ( fsTableModel.get("deprecated") ) {
                return;
            }

            const fsTableIdentify = fsTableModel.getIdentify();
            const dbTableModel = db.tables.getByIdentify(fsTableIdentify);

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
                if ( options.mode === "dev" ) {
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
        db.tables.each((dbTableModel) => {
            const dbTableIdentify = dbTableModel.getIdentify();
            const fsTableModel = fs.tables.getByIdentify(dbTableIdentify);

            if ( fsTableModel ) {
                return;
            }

            if ( options.mode === "dev" ) {
                const errorModel = new CannotDropTableErrorModel({
                    filePath: "(database)",
                    tableIdentify: dbTableIdentify
                });
                errors.push(errorModel);
            }
        });

        // drop trigger
        db.triggers.each((dbTriggerModel) => {
            const dbTriggerIdentify = dbTriggerModel.getIdentify();
            const fsTriggerModel = fs.triggers.getByIdentify(dbTriggerIdentify);

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
        fs.triggers.each((fsTriggerModel) => {
            const fsTriggerIdentify = fsTriggerModel.getIdentify();
            const dbTriggerModel = db.triggers.getByIdentify(fsTriggerIdentify);

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
            const fsFunctionModel = fs.functions.getByIdentify(functionIdentify);
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
            const fsTableModel = fs.tables.getByIdentify(tableIdentify);

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

        // output migration
        return new MigrationModel({
            commands,
            errors
        });
    }
}