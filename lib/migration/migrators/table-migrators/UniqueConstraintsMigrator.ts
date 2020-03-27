import { BaseConstraintMigrator } from "./BaseConstraintMigrator";
import { UniqueConstraintCommandModel } from "../../commands/UniqueConstraintCommandModel";
import { UniqueConstraintModel } from "../../../objects/UniqueConstraintModel";

export class UniqueConstraintsMigrator
extends BaseConstraintMigrator<UniqueConstraintModel> {
    
    protected getFSConstraints(): UniqueConstraintModel[] {
        return this.fsTableModel.get("uniqueConstraints");
    }

    protected getDBConstraints(): UniqueConstraintModel[] {
        return this.dbTableModel.get("uniqueConstraints");
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