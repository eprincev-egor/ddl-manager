import {TableModel} from "../../../objects/TableModel";
import { IBaseMigratorParams } from "./BaseMigrator";
import { MigrationModel, InputCommand } from "../../MigrationModel";
import { FSDDLState } from "../../../state/FSDDLState";
import { DDLState } from "../../../state/DDLState";
import { BaseDBObjectModel } from "../../../objects/base-layers/BaseDBObjectModel";

export abstract class ConstraintMigrator<ConstraintModel extends BaseDBObjectModel<any>> {
    protected migration: MigrationModel;
    protected fs: FSDDLState;
    protected db: DDLState;
    protected fsTableModel: TableModel;
    protected dbTableModel: TableModel;

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
        
        const fsConstraints = this.getFSConstraints();
        const dbConstraints = this.getDBConstraints();

        for (const fsConstraint of fsConstraints) {
            const name = fsConstraint.get("name");
            const existsDbConstraint = dbConstraints.find(dbConstraint =>
                dbConstraint.get("name") === name
            );

            if ( existsDbConstraint ) {
                const isEqual = existsDbConstraint.equal(fsConstraint);
                
                if ( !isEqual ) {
                    this.drop(existsDbConstraint);
                    this.create(fsConstraint);
                }
            }
            else {
                this.create(fsConstraint);
            }
        }

        for (const dbConstraint of dbConstraints) {
            const name = dbConstraint.get("name");
            const existsFsConstraint = fsConstraints.find(fsConstraint =>
                fsConstraint.get("name") === name
            );

            if ( !existsFsConstraint ) {
                this.drop(dbConstraint);
            }
        }
    }

    private drop(constraint: ConstraintModel) {
        const dropConstraintCommand = this.createDropCommand(constraint);
        this.migration.addCommand(dropConstraintCommand);

    }

    private create(constraint: ConstraintModel) {
        const createConstraintCommand = this.createCreateCommand(constraint);
        this.migration.addCommand(createConstraintCommand);
    }

    protected abstract getFSConstraints(): ConstraintModel[];
    protected abstract getDBConstraints(): ConstraintModel[];

    protected abstract createDropCommand(constraint: ConstraintModel): InputCommand;
    protected abstract createCreateCommand(constraint: ConstraintModel): InputCommand;
}