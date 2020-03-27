import { BaseConstraintMigrator } from "./BaseConstraintMigrator";
import { CheckConstraintCommandModel } from "../../commands/CheckConstraintCommandModel";
import { CheckConstraintModel } from "../../../objects/CheckConstraintModel";

export class CheckConstraintsMigrator
extends BaseConstraintMigrator<CheckConstraintModel> {
    
    protected getFSConstraints(): CheckConstraintModel[] {
        return this.fsTableModel.get("checkConstraints");
    }

    protected getDBConstraints(): CheckConstraintModel[] {
        return this.dbTableModel.get("checkConstraints");
    }

    protected createDropCommand(constraint: CheckConstraintModel) {
        const dropConstraintCommand = new CheckConstraintCommandModel({
            type: "drop",
            tableIdentify: this.dbTableModel.getIdentify(),
            constraint
        });
        return dropConstraintCommand;
    }

    protected createCreateCommand(constraint: CheckConstraintModel) {
        const dropConstraintCommand = new CheckConstraintCommandModel({
            type: "create",
            tableIdentify: this.fsTableModel.getIdentify(),
            constraint
        });
        return dropConstraintCommand;
    }
}