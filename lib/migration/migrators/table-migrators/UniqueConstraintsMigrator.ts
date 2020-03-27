import { ConstraintMigrator } from "../base-layers/ConstraintMigrator";
import { UniqueConstraintCommandModel } from "../../commands/UniqueConstraintCommandModel";
import { UniqueConstraintModel } from "../../../objects/UniqueConstraintModel";

export class UniqueConstraintsMigrator
extends ConstraintMigrator<UniqueConstraintModel> {
    
    protected calcChanges() {
        const changes = this.fsTableModel.compareConstraintsWithDBTable<UniqueConstraintModel>(
            "uniqueConstraints", 
            this.dbTableModel
        );
        return changes;
    }

    protected createDropCommand(constraint: UniqueConstraintModel) {
        const dropConstraintCommand = new UniqueConstraintCommandModel({
            type: "drop",
            tableIdentify: this.dbTableModel.getIdentify(),
            unique: constraint
        });
        return dropConstraintCommand;
    }

    protected createCreateCommand(constraint: UniqueConstraintModel) {
        const dropConstraintCommand = new UniqueConstraintCommandModel({
            type: "create",
            tableIdentify: this.dbTableModel.getIdentify(),
            unique: constraint
        });
        return dropConstraintCommand;
    }
}