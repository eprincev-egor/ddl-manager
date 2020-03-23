import BaseController from "./base-layers/BaseController";

import PrimaryKeyCommandModel from "../commands/PrimaryKeyCommandModel";
import CheckConstraintCommandModel from "../commands/CheckConstraintCommandModel";
import UniqueConstraintCommandModel from "../commands/UniqueConstraintCommandModel";
import ForeignKeyConstraintCommandModel from "../commands/ForeignKeyConstraintCommandModel";
import ReferenceToUnknownTableErrorModel from "../errors/ReferenceToUnknownTableErrorModel";
import ReferenceToUnknownColumnErrorModel from "../errors/ReferenceToUnknownColumnErrorModel";
import TableModel from "../../objects/TableModel";

export default class TableConstraintController extends BaseController {

    generateConstraintMigration(
        fsTableModel: TableModel,
        dbTableModel: TableModel
    ) {
        const fsTableIdentify = fsTableModel.get("identify");

        // create/drop primary key
        const fsPrimaryKey = fsTableModel.get("primaryKey");
        const dbPrimaryKey = dbTableModel.get("primaryKey");

        if ( fsPrimaryKey && !dbPrimaryKey ) {
            const primaryKeyCommand = new PrimaryKeyCommandModel({
                type: "create",
                tableIdentify: fsTableIdentify,
                primaryKey: fsPrimaryKey
            });
            this.migration.addCommand(primaryKeyCommand);
        }

        if ( !fsPrimaryKey && dbPrimaryKey ) {
            const primaryKeyCommand = new PrimaryKeyCommandModel({
                type: "drop",
                tableIdentify: fsTableIdentify,
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
                    tableIdentify: fsTableIdentify,
                    primaryKey: dbPrimaryKey
                });
                this.migration.addCommand(dropPrimaryKeyCommand);

                const createPrimaryKeyCommand = new PrimaryKeyCommandModel({
                    type: "create",
                    tableIdentify: fsTableIdentify,
                    primaryKey: fsPrimaryKey
                });
                this.migration.addCommand(createPrimaryKeyCommand);
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
                    this.migration.addCommand(dropConstraintCommand);

                    const createConstraintCommand = new CheckConstraintCommandModel({
                        type: "create",
                        tableIdentify: fsTableIdentify,
                        constraint: fsConstraint
                    });
                    this.migration.addCommand(createConstraintCommand);
                }
            }
            else {
                const constraintCommand = new CheckConstraintCommandModel({
                    type: "create",
                    tableIdentify: fsTableIdentify,
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
                    tableIdentify: fsTableIdentify,
                    constraint: dbConstraint
                });
                this.migration.addCommand(constraintCommand);
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
                    this.migration.addCommand(dropConstraintCommand);

                    const createConstraintCommand = new UniqueConstraintCommandModel({
                        type: "create",
                        tableIdentify: fsTableIdentify,
                        unique: fsConstraint
                    });
                    this.migration.addCommand(createConstraintCommand);
                }
            }
            else {
                const constraintCommand = new UniqueConstraintCommandModel({
                    type: "create",
                    tableIdentify: fsTableIdentify,
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
                    tableIdentify: fsTableIdentify,
                    unique: dbConstraint
                });
                this.migration.addCommand(constraintCommand);
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
                    filePath: fsTableModel.get("filePath"),
                    foreignKeyName: name,
                    tableIdentify: fsTableIdentify,
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
                        tableIdentify: fsTableIdentify,
                        foreignKey: existsDbConstraint
                    });
                    this.migration.addCommand(dropConstraintCommand);

                    const createConstraintCommand = new ForeignKeyConstraintCommandModel({
                        type: "create",
                        tableIdentify: fsTableIdentify,
                        foreignKey: fsConstraint
                    });
                    this.migration.addCommand(createConstraintCommand);
                }
            }
            else {
                const constraintCommand = new ForeignKeyConstraintCommandModel({
                    type: "create",
                    tableIdentify: fsTableIdentify,
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
                    tableIdentify: fsTableIdentify,
                    foreignKey: dbConstraint
                });
                this.migration.addCommand(constraintCommand);
            }
        }
    }
}