import {BaseValidationsController} from "./BaseValidationsController";
import {InputCommand} from "../../MigrationModel";
import { IChanges } from "../../../objects/base-layers/BaseDBObjectCollection";
import {NamedAndMovableDBOModel} from "../../../objects/base-layers/NamedAndMovableDBOModel";

export 
abstract class BaseStrategyController<DBOModel extends NamedAndMovableDBOModel<any>>
extends BaseValidationsController {
    
    generate() {
        const {
            created,
            removed,
            changed
        } = this.detectChanges();
        
        removed.forEach((dbo) => {
            if ( dbo.allowedToDrop() ) {
                this.drop(dbo);
            }
        });

        changed.forEach(({prev, next}) => {
            this.drop(prev);
            this.tryCreate(next);
        });

        created.forEach((dbo) => {
            this.tryCreate(dbo);
        });
    }

    private drop(dbo: DBOModel) {
        const dropCommand = this.getDropCommand(dbo);
        this.migration.addCommand(dropCommand);
    }

    private tryCreate(dbo: DBOModel) {
        try {
            this.validate(dbo);
            this.create(dbo);
        } catch(err) {
            if ( !this.isValidationError(err) ) {
                throw err;
            }
        }
    }

    private create(dbo: DBOModel) {
        const createCommand = this.getCreateCommand(dbo);
        this.migration.addCommand(createCommand);
    }

    protected abstract getDropCommand(dbo: DBOModel): InputCommand;
    protected abstract getCreateCommand(dbo: DBOModel): InputCommand;
    protected abstract validate(dbo: DBOModel): void;
    protected abstract detectChanges(): IChanges<DBOModel>;
}