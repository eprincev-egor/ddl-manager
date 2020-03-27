import {TableModel} from "../../../objects/TableModel";
import { IBaseMigratorParams } from "./BaseMigrator";
import { MigrationModel, InputCommand } from "../../MigrationModel";
import { FSDDLState } from "../../../state/FSDDLState";
import { DDLState } from "../../../state/DDLState";
import { BaseDBObjectModel } from "../../../objects/base-layers/BaseDBObjectModel";
import { IChanges } from "../../../state/Changes";

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
        
        const changes = this.calcChanges();

        changes.removed.forEach((constraint) => {
            this.drop(constraint);
        });
        changes.changed.forEach(({prev, next}) => {
            const dbConstraint = prev;
            const fsConstraint = next;
            this.drop(dbConstraint);
            this.create(fsConstraint);
        });
        changes.created.forEach((constraint) => {
            this.create(constraint);
        });
        
    }

    private drop(constraint: ConstraintModel) {
        const dropConstraintCommand = this.createDropCommand(constraint);
        this.migration.addCommand(dropConstraintCommand);

    }

    private create(constraint: ConstraintModel) {
        const createConstraintCommand = this.createCreateCommand(constraint);
        this.migration.addCommand(createConstraintCommand);
    }

    protected abstract calcChanges(): IChanges<ConstraintModel>;
    protected abstract createDropCommand(constraint: ConstraintModel): InputCommand;
    protected abstract createCreateCommand(constraint: ConstraintModel): InputCommand;
}