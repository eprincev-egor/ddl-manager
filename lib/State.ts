import {Model, Types} from "model-layer";
import FunctionsCollection from "./objects/FunctionsCollection";
import TriggersCollection from "./objects/TriggersCollection";
import ViewsCollection from "./objects/ViewsCollection";
import TablesCollection from "./objects/TablesCollection";
import Migration from "./migration/Migration";
import CommandsCollection from "./migration/commands/CommandsCollection";
import FunctionCommandModel from "./migration/commands/FunctionCommandModel";
import ViewCommandModel from "./migration/commands/ViewCommandModel";
import TableCommandModel from "./migration/commands/TableCommandModel";
import ColumnCommandModel from "./migration/commands/ColumnCommandModel";
import TriggerCommandModel from "./migration/commands/TriggerCommandModel";
import MigrationErrorsCollection from "./migration/errors/MigrationErrorsCollection";
import UnknownTableForTriggerErrorModel from "./migration/errors/UnknownTableForTriggerErrorModel";
import UnknownFunctionForTriggerErrorModel from "./migration/errors/UnknownFunctionForTriggerErrorModel";
import MaxObjectNameSizeErrorModel from "./migration/errors/MaxObjectNameSizeErrorModel";
import CannotDropColumnErrorModel from "./migration/errors/CannotDropColumnErrorModel";

export default class State<Child extends State = State<any>> extends Model<Child> {
    structure() {
        return {
            functions: Types.Collection({
                Collection: FunctionsCollection,
                default: () => new FunctionsCollection()
            }),
            triggers: Types.Collection({
                Collection: TriggersCollection,
                default: () => new TriggersCollection()
            }),
            views: Types.Collection({
                Collection: ViewsCollection,
                default: () => new ViewsCollection()
            }),
            tables: Types.Collection({
                Collection: TablesCollection,
                default: () => new TablesCollection()
            })
        };
    }

    generateMigration(dbState: State): Migration {
        const fsState: State = this;
        const fs = {
            functions: fsState.get("functions"),
            views: fsState.get("views"),
            tables: fsState.get("tables"),
            triggers: fsState.get("triggers")
        };
        const db = {
            functions: dbState.get("functions"),
            views: dbState.get("views"),
            tables: dbState.get("tables"),
            triggers: dbState.get("triggers")
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
                dbColumns.forEach((dbColumnModel) => {
                    const key = dbColumnModel.get("key");
                    const existsFsColumn = fsColumns.find((fsColumn) =>
                        fsColumn.get("key") === key
                    );

                    if ( existsFsColumn ) {
                        return;
                    }

                    const errorModel = new CannotDropColumnErrorModel({
                        filePath: fsTableModel.get("filePath"),
                        tableIdentify: fsTableIdentify,
                        columnKey: key
                    });
                    errors.push(errorModel);
                });

                return;
            }

            const createTableCommand = new TableCommandModel({
                type: "create",
                table: fsTableModel
            });
            commands.push(createTableCommand);
        });

        // drop trigger
        db.triggers.each((dbTriggerModel) => {
            const dbTriggerIdentify = dbTriggerModel.getIdentify();
            const fsTriggerModel = fs.views.getByIdentify(dbTriggerIdentify);

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
            const dbTriggerModel = db.views.getByIdentify(fsTriggerIdentify);

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
        return new Migration({
            commands,
            errors
        });
    }
}