import { BaseMigrator, IBaseMigratorParams } from "./BaseMigrator";
import { InputCommand } from "../../MigrationModel";
import { BaseValidator } from "../validators/BaseValidator";
import { BaseDBObjectModel } from "../../../objects/base-layers/BaseDBObjectModel";

export interface IAllowedToDrop {
    allowedToDrop(): boolean;
}

export 
abstract class SimpleMigrator<
    DBOModel extends BaseDBObjectModel<any> & IAllowedToDrop
>
extends BaseMigrator<DBOModel> {
    
    protected validators: BaseValidator[];

    constructor(params: IBaseMigratorParams) {
        super(params);
        this.validators = this.createValidators(params);
    }

    protected createValidators(params: IBaseMigratorParams): BaseValidator[] {
        const Validators = this.getValidators();

        return Validators.map((Validator) => {
            const validator = new Validator(params);
            return validator;
        })
    }

    protected getValidators(): (new (...args: any) => BaseValidator)[] {
        return [];
    }

    protected onRemove(dbo: DBOModel) {
        if ( dbo.allowedToDrop() ) {
            this.drop(dbo);
        }
    }

    protected onChange(oldDBO: DBOModel, newDBO: DBOModel) {
        this.drop(oldDBO);
        this.tryCreate(newDBO);
    }

    protected onCreate(dbo: DBOModel) {
        this.tryCreate(dbo);
    }
    
    protected tryCreate(dbo: DBOModel) {
        const isValid = this.validate(dbo);
        if ( isValid ) {
            this.create(dbo);
        }
    }

    private drop(dbo: DBOModel) {
        const dropCommand = this.createDropCommand(dbo);
        this.migration.addCommand(dropCommand);
    }

    private create(dbo: DBOModel) {
        const createCommand = this.createCreateCommand(dbo);
        this.migration.addCommand(createCommand);
    }

    protected validate(dbo: DBOModel): boolean {
        for (const validator of this.validators) {
            const errorModel = validator.validate(dbo);

            if ( errorModel ) {
                this.migration.addError(errorModel);
                return false;
            }
        }

        return true;
    }

    protected abstract createDropCommand(dbo: DBOModel): InputCommand;
    protected abstract createCreateCommand(dbo: DBOModel): InputCommand;
}