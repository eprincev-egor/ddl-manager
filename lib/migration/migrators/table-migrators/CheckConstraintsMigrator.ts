import { ConstraintMigrator } from "../base-layers/ConstraintMigrator";
import { CheckConstraintCommandModel } from "../../commands/CheckConstraintCommandModel";
import { CheckConstraintModel } from "../../../objects/CheckConstraintModel";

export class CheckConstraintsMigrator
extends ConstraintMigrator<CheckConstraintModel> {
    
    protected calcChanges() {
        const changes = this.fsTableModel.compareConstraintsWithDBTable<CheckConstraintModel>(
            "checkConstraints", 
            this.dbTableModel
        );
        return changes;
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