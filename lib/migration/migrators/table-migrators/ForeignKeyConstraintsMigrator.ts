
import {ForeignKeyConstraintCommandModel} from "../../commands/ForeignKeyConstraintCommandModel";
import {ReferenceToUnknownTableErrorModel} from "../../errors/ReferenceToUnknownTableErrorModel";
import {ReferenceToUnknownColumnErrorModel} from "../../errors/ReferenceToUnknownColumnErrorModel";
import {TableModel} from "../../../objects/TableModel";
import { IBaseMigratorParams } from "../base-layers/BaseMigrator";
import { MigrationModel } from "../../MigrationModel";
import { FSDDLState } from "../../../state/FSDDLState";
import { DDLState } from "../../../state/DDLState";

export class ForeignKeyConstraintsMigrator {
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
        
        // create/drop foreign key constraints
        this.migrateForeignKeyConstraints();
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