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
import ExpectedPrimaryKeyForRowsErrorModel from "./errors/ExpectedPrimaryKeyForRowsErrorModel";
import CreateRowsCommandModel from "./commands/RowsCommandModel";
import ColumnNotNullCommandModel from "./commands/ColumnNotNullCommandModel";
import PrimaryKeyCommandModel from "./commands/PrimaryKeyCommandModel";
import CheckConstraintCommandModel from "./commands/CheckConstraintCommandModel";
import UniqueConstraintCommandModel from "./commands/UniqueConstraintCommandModel";
import ForeignKeyConstraintCommandModel from "./commands/ForeignKeyConstraintCommandModel";
import ReferenceToUnknownTableErrorModel from "./errors/ReferenceToUnknownTableErrorModel";
import ReferenceToUnknownColumnErrorModel from "./errors/ReferenceToUnknownColumnErrorModel";

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

            if ( fsTableModel.get("rows") ) {
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

                // create/drop primary key
                const fsPrimaryKey = fsTableModel.get("primaryKey");
                const dbPrimaryKey = dbTableModel.get("primaryKey");

                if ( fsPrimaryKey && !dbPrimaryKey ) {
                    const primaryKeyCommand = new PrimaryKeyCommandModel({
                        type: "create",
                        tableIdentify: fsTableIdentify,
                        primaryKey: fsPrimaryKey
                    });
                    commands.push(primaryKeyCommand);
                }

                if ( !fsPrimaryKey && dbPrimaryKey ) {
                    const primaryKeyCommand = new PrimaryKeyCommandModel({
                        type: "drop",
                        tableIdentify: fsTableIdentify,
                        primaryKey: dbPrimaryKey
                    });
                    commands.push(primaryKeyCommand);
                }

                if ( fsPrimaryKey && dbPrimaryKey ) {
                    const isEqual = (
                        fsPrimaryKey.length === dbPrimaryKey.length &&
                        fsPrimaryKey.every(key => dbPrimaryKey.includes(key))
                    );

                    if ( !isEqual ) {
                        const dropPrimaryKeyCommand = new PrimaryKeyCommandModel({
                            type: "drop",
                            tableIdentify: fsTableIdentify,
                            primaryKey: dbPrimaryKey
                        });
                        commands.push(dropPrimaryKeyCommand);

                        const createPrimaryKeyCommand = new PrimaryKeyCommandModel({
                            type: "create",
                            tableIdentify: fsTableIdentify,
                            primaryKey: fsPrimaryKey
                        });
                        commands.push(createPrimaryKeyCommand);
                    }
                }

                // create/drop check constraints
                const fsCheckConstraints = fsTableModel.get("checkConstraints");
                const dbCheckConstraints = dbTableModel.get("checkConstraints");

                for (const fsConstraint of fsCheckConstraints) {
                    const name = fsConstraint.get("name");
                    const existsDbConstraint = dbCheckConstraints.find(dbConstraint =>
                        dbConstraint.get("name") === name
                    );

                    if ( existsDbConstraint ) {
                        const isEqual = existsDbConstraint.equal(fsConstraint);
                        
                        if ( !isEqual ) {
                            const dropConstraintCommand = new CheckConstraintCommandModel({
                                type: "drop",
                                tableIdentify: fsTableIdentify,
                                constraint: existsDbConstraint
                            });
                            commands.push(dropConstraintCommand);

                            const createConstraintCommand = new CheckConstraintCommandModel({
                                type: "create",
                                tableIdentify: fsTableIdentify,
                                constraint: fsConstraint
                            });
                            commands.push(createConstraintCommand);
                        }
                    }
                    else {
                        const constraintCommand = new CheckConstraintCommandModel({
                            type: "create",
                            tableIdentify: fsTableIdentify,
                            constraint: fsConstraint
                        });
                        commands.push(constraintCommand);
                    }
                }

                for (const dbConstraint of dbCheckConstraints) {
                    const name = dbConstraint.get("name");
                    const existsFsConstraint = fsCheckConstraints.find(fsConstraint =>
                        fsConstraint.get("name") === name
                    );

                    if ( !existsFsConstraint ) {
                        const constraintCommand = new CheckConstraintCommandModel({
                            type: "drop",
                            tableIdentify: fsTableIdentify,
                            constraint: dbConstraint
                        });
                        commands.push(constraintCommand);
                    }
                }

                
                // create/drop unique constraints
                const fsUniqueConstraints = fsTableModel.get("uniqueConstraints");
                const dbUniqueConstraints = dbTableModel.get("uniqueConstraints");

                for (const fsConstraint of fsUniqueConstraints) {
                    const name = fsConstraint.get("name");
                    const existsDbConstraint = dbUniqueConstraints.find(dbConstraint =>
                        dbConstraint.get("name") === name
                    );

                    if ( existsDbConstraint ) {
                        const isEqual = existsDbConstraint.equal(fsConstraint);
                        
                        if ( !isEqual ) {
                            const dropConstraintCommand = new UniqueConstraintCommandModel({
                                type: "drop",
                                tableIdentify: fsTableIdentify,
                                unique: existsDbConstraint
                            });
                            commands.push(dropConstraintCommand);

                            const createConstraintCommand = new UniqueConstraintCommandModel({
                                type: "create",
                                tableIdentify: fsTableIdentify,
                                unique: fsConstraint
                            });
                            commands.push(createConstraintCommand);
                        }
                    }
                    else {
                        const constraintCommand = new UniqueConstraintCommandModel({
                            type: "create",
                            tableIdentify: fsTableIdentify,
                            unique: fsConstraint
                        });
                        commands.push(constraintCommand);
                    }
                }

                for (const dbConstraint of dbUniqueConstraints) {
                    const name = dbConstraint.get("name");
                    const existsFsConstraint = fsUniqueConstraints.find(fsConstraint =>
                        fsConstraint.get("name") === name
                    );

                    if ( !existsFsConstraint ) {
                        const constraintCommand = new UniqueConstraintCommandModel({
                            type: "drop",
                            tableIdentify: fsTableIdentify,
                            unique: dbConstraint
                        });
                        commands.push(constraintCommand);
                    }
                }


                // create/drop foreign key constraints
                const fsForeignKeyConstraints = fsTableModel.get("foreignKeysConstraints");
                const dbForeignKeyConstraints = dbTableModel.get("foreignKeysConstraints");

                for (const fsConstraint of fsForeignKeyConstraints) {
                    const name = fsConstraint.get("name");

                    // validate
                    const referenceTableIdentify = fsConstraint.get("referenceTableIdentify");
                    const referenceTableModel = this.db.row.tables.getByIdentify(referenceTableIdentify);

                    if ( !referenceTableModel ) {
                        const errorModel = new ReferenceToUnknownTableErrorModel({
                            filePath: fsTableModel.get("filePath"),
                            foreignKeyName: name,
                            tableIdentify: fsTableIdentify,
                            referenceTableIdentify
                        });
                        errors.push(errorModel);
                        continue;
                    }

                    const referenceColumns = fsConstraint.get("referenceColumns");
                    const unknownColumns = [];
                    referenceColumns.forEach(key => {
                        const existsColumn = referenceTableModel.get("columns").find(column =>
                            column.get("key") === key
                        );

                        if ( !existsColumn ) {
                            unknownColumns.push(key);
                        }
                    });
                    if ( unknownColumns.length ) {
                        const errorModel = new ReferenceToUnknownColumnErrorModel({
                            filePath: fsTableModel.get("filePath"),
                            foreignKeyName: name,
                            tableIdentify: fsTableIdentify,
                            referenceTableIdentify,
                            referenceColumns: unknownColumns
                        });
                        errors.push(errorModel);
                        continue;
                    }

                    // migrate
                    const existsDbConstraint = dbForeignKeyConstraints.find(dbConstraint =>
                        dbConstraint.get("name") === name
                    );

                    if ( existsDbConstraint ) {
                        const isEqual = existsDbConstraint.equal(fsConstraint);
                        
                        if ( !isEqual ) {
                            const dropConstraintCommand = new ForeignKeyConstraintCommandModel({
                                type: "drop",
                                tableIdentify: fsTableIdentify,
                                foreignKey: existsDbConstraint
                            });
                            commands.push(dropConstraintCommand);

                            const createConstraintCommand = new ForeignKeyConstraintCommandModel({
                                type: "create",
                                tableIdentify: fsTableIdentify,
                                foreignKey: fsConstraint
                            });
                            commands.push(createConstraintCommand);
                        }
                    }
                    else {
                        const constraintCommand = new ForeignKeyConstraintCommandModel({
                            type: "create",
                            tableIdentify: fsTableIdentify,
                            foreignKey: fsConstraint
                        });
                        commands.push(constraintCommand);
                    }
                }

                for (const dbConstraint of dbForeignKeyConstraints) {
                    const name = dbConstraint.get("name");
                    const existsFsConstraint = fsForeignKeyConstraints.find(fsConstraint =>
                        fsConstraint.get("name") === name
                    );

                    if ( !existsFsConstraint ) {
                        const constraintCommand = new ForeignKeyConstraintCommandModel({
                            type: "drop",
                            tableIdentify: fsTableIdentify,
                            foreignKey: dbConstraint
                        });
                        commands.push(constraintCommand);
                    }
                }
            }
            else {
                const createTableCommand = new TableCommandModel({
                    type: "create",
                    table: fsTableModel
                });
                commands.push(createTableCommand);
    
            }

            // (re)create table rows
            if ( fsTableModel.get("rows") ) {
                const createRowsCommand = new CreateRowsCommandModel({
                    type: "create",
                    table: fsTableModel,
                    rows: fsTableModel.get("rows")
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