import {PrimaryKeyCommandModel} from "../../commands/PrimaryKeyCommandModel";
import {CheckConstraintCommandModel} from "../../commands/CheckConstraintCommandModel";
import {UniqueConstraintCommandModel} from "../../commands/UniqueConstraintCommandModel";
import {ForeignKeyConstraintCommandModel} from "../../commands/ForeignKeyConstraintCommandModel";
import {ReferenceToUnknownTableErrorModel} from "../../errors/ReferenceToUnknownTableErrorModel";
import {ReferenceToUnknownColumnErrorModel} from "../../errors/ReferenceToUnknownColumnErrorModel";
import {TableModel} from "../../../objects/TableModel";
import { IBaseMigratorParams } from "../base-layers/BaseMigrator";
import { MigrationModel } from "../../MigrationModel";
import { FSDDLState } from "../../../state/FSDDLState";
import { DDLState } from "../../../state/DDLState";

export class TableConstraintsMigrator {
    protected migration: MigrationModel;
    protected fs: FSDDLState;
    protected db: DDLState;
    private fsTableModel: TableModel;
    private dbTableModel: TableModel;

    constructor(params: IBaseMigratorParams) {
        this.fs = params.fs;
        this.db = params.db;
    }

    migrate(
        migration: MigrationModel,
        fsTableModel: TableModel,
        dbTableModel: TableModel
    ) {
        this.migration = migration;
        this.fsTableModel = fsTableModel;
        this.dbTableModel = dbTableModel;
        
        // create/drop primary key
        this.migratePrimaryKey();

        // create/drop check constraints
        this.migrateCheckConstraints();

        // create/drop unique constraints
        this.migrateUniqueConstraints();

        // create/drop foreign key constraints
        this.migrateForeignKeyConstraints();
    }

    private migratePrimaryKey() {
        const tableIdentify = this.fsTableModel.getIdentify();

        const fsPrimaryKey = this.fsTableModel.get("primaryKey");
        const dbPrimaryKey = this.dbTableModel.get("primaryKey");

        if ( fsPrimaryKey && !dbPrimaryKey ) {
            const primaryKeyCommand = new PrimaryKeyCommandModel({
                type: "create",
                tableIdentify,
                primaryKey: fsPrimaryKey
            });
            this.migration.addCommand(primaryKeyCommand);
        }

        if ( !fsPrimaryKey && dbPrimaryKey ) {
            const primaryKeyCommand = new PrimaryKeyCommandModel({
                type: "drop",
                tableIdentify,
                primaryKey: dbPrimaryKey
            });
            this.migration.addCommand(primaryKeyCommand);
        }

        if ( fsPrimaryKey && dbPrimaryKey ) {
            const isEqual = (
                fsPrimaryKey.length === dbPrimaryKey.length &&
                fsPrimaryKey.every(key => dbPrimaryKey.includes(key))
            );

            if ( !isEqual ) {
                const dropPrimaryKeyCommand = new PrimaryKeyCommandModel({
                    type: "drop",
                    tableIdentify,
                    primaryKey: dbPrimaryKey
                });
                this.migration.addCommand(dropPrimaryKeyCommand);

                const createPrimaryKeyCommand = new PrimaryKeyCommandModel({
                    type: "create",
                    tableIdentify,
                    primaryKey: fsPrimaryKey
                });
                this.migration.addCommand(createPrimaryKeyCommand);
            }
        }

    }

    private migrateCheckConstraints() {
        const tableIdentify = this.fsTableModel.getIdentify();

        const fsCheckConstraints = this.fsTableModel.get("checkConstraints");
        const dbCheckConstraints = this.dbTableModel.get("checkConstraints");

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
                        tableIdentify,
                        constraint: existsDbConstraint
                    });
                    this.migration.addCommand(dropConstraintCommand);

                    const createConstraintCommand = new CheckConstraintCommandModel({
                        type: "create",
                        tableIdentify,
                        constraint: fsConstraint
                    });
                    this.migration.addCommand(createConstraintCommand);
                }
            }
            else {
                const constraintCommand = new CheckConstraintCommandModel({
                    type: "create",
                    tableIdentify,
                    constraint: fsConstraint
                });
                this.migration.addCommand(constraintCommand);
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
                    tableIdentify,
                    constraint: dbConstraint
                });
                this.migration.addCommand(constraintCommand);
            }
        }
    }

    private migrateUniqueConstraints() {
        const tableIdentify = this.fsTableModel.getIdentify();

        const fsUniqueConstraints = this.fsTableModel.get("uniqueConstraints");
        const dbUniqueConstraints = this.dbTableModel.get("uniqueConstraints");

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
                        tableIdentify,
                        unique: existsDbConstraint
                    });
                    this.migration.addCommand(dropConstraintCommand);

                    const createConstraintCommand = new UniqueConstraintCommandModel({
                        type: "create",
                        tableIdentify,
                        unique: fsConstraint
                    });
                    this.migration.addCommand(createConstraintCommand);
                }
            }
            else {
                const constraintCommand = new UniqueConstraintCommandModel({
                    type: "create",
                    tableIdentify,
                    unique: fsConstraint
                });
                this.migration.addCommand(constraintCommand);
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
                    tableIdentify,
                    unique: dbConstraint
                });
                this.migration.addCommand(constraintCommand);
            }
        }
    }

    private migrateForeignKeyConstraints() {
        const tableIdentify = this.fsTableModel.getIdentify();

        const fsForeignKeyConstraints = this.fsTableModel.get("foreignKeysConstraints");
        const dbForeignKeyConstraints = this.dbTableModel.get("foreignKeysConstraints");

        for (const fsConstraint of fsForeignKeyConstraints) {
            const name = fsConstraint.get("name");

            // validate
            const referenceTableIdentify = fsConstraint.get("referenceTableIdentify");
            const referenceTableModel = this.db.row.tables.getByIdentify(referenceTableIdentify);

            if ( !referenceTableModel ) {
                const errorModel = new ReferenceToUnknownTableErrorModel({
                    filePath: this.fsTableModel.get("filePath"),
                    foreignKeyName: name,
                    tableIdentify,
                    referenceTableIdentify
                });
                this.migration.addError(errorModel);
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
                    filePath: this.fsTableModel.get("filePath"),
                    foreignKeyName: name,
                    tableIdentify,
                    referenceTableIdentify,
                    referenceColumns: unknownColumns
                });
                this.migration.addError(errorModel);
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
                        tableIdentify,
                        foreignKey: existsDbConstraint
                    });
                    this.migration.addCommand(dropConstraintCommand);

                    const createConstraintCommand = new ForeignKeyConstraintCommandModel({
                        type: "create",
                        tableIdentify,
                        foreignKey: fsConstraint
                    });
                    this.migration.addCommand(createConstraintCommand);
                }
            }
            else {
                const constraintCommand = new ForeignKeyConstraintCommandModel({
                    type: "create",
                    tableIdentify,
                    foreignKey: fsConstraint
                });
                this.migration.addCommand(constraintCommand);
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
                    tableIdentify,
                    foreignKey: dbConstraint
                });
                this.migration.addCommand(constraintCommand);
            }
        }
    }
}