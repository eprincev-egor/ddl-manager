
import { ForeignKeyConstraintCommandModel } from "../../commands/ForeignKeyConstraintCommandModel";
import { ForeignKeyConstraintModel } from "../../../objects/ForeignKeyConstraintModel";
import { ConstraintMigrator } from "../base-layers/ConstraintMigrator";
import { ForeignKeyValidator } from "../validators/ForeignKeyValidator";
import { IBaseMigratorParams } from "../base-layers/BaseMigrator";

export class ForeignKeyConstraintsMigrator
extends ConstraintMigrator<ForeignKeyConstraintModel> {

    private fkValidator: ForeignKeyValidator;

    constructor(params: IBaseMigratorParams) {
        super(params);
        this.fkValidator = new ForeignKeyValidator(params);
    }

    protected validate(fk: ForeignKeyConstraintModel) {
        const errorModel = this.fkValidator.validateTable(
            this.fsTableModel, fk
        );

        if ( errorModel ) {
            this.migration.addError(errorModel);
            return false;
        }

        return true;
    }

    protected calcChanges() {
        const changes = this.fsTableModel.compareConstraintsWithDBTable<ForeignKeyConstraintModel>(
            "foreignKeysConstraints", 
            this.dbTableModel
        );
        return changes;
    }

    protected createDropCommand(fk: ForeignKeyConstraintModel) {
        const dropConstraintCommand = new ForeignKeyConstraintCommandModel({
            type: "drop",
            tableIdentify: this.dbTableModel.getIdentify(),
            foreignKey: fk
        });
        return dropConstraintCommand;
    }

    protected createCreateCommand(fk: ForeignKeyConstraintModel) {
        const dropConstraintCommand = new ForeignKeyConstraintCommandModel({
            type: "create",
            tableIdentify: this.fsTableModel.getIdentify(),
            foreignKey: fk
        });
        return dropConstraintCommand;
    }
}