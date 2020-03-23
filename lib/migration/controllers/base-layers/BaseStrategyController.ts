import BaseValidationsController from "./BaseValidationsController";
import {InputCommand, InputError} from "../../MigrationModel";
import { IChanges } from "../../../objects/BaseDBObjectCollection";
import NamedAndMovableDBOModel from "../../../objects/NamedAndMovableDBOModel";

export default 
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
            this.create(next);
        });

        created.forEach((dbo) => {
            const errors = this.validate(dbo)
                .filter(err => !!err);
            
            if ( errors.length ) {
                this.saveErrors(errors);
                return;
            }

            this.create(dbo);
        });
    }

    drop(dbo: DBOModel) {
        const dropCommand = this.getDropCommand(dbo);
        this.migration.addCommand(dropCommand);
    }

    create(dbo: DBOModel) {
        const createCommand = this.getCreateCommand(dbo);
        this.migration.addCommand(createCommand);
    }

    saveErrors(errors: InputError[]) {
        for (const err of errors) {
            this.migration.addError(err);
        }
    }

    abstract getDropCommand(dbo: DBOModel): InputCommand;
    abstract getCreateCommand(dbo: DBOModel): InputCommand;
    abstract validate(dbo: DBOModel): (InputError | undefined)[];
    abstract detectChanges(): IChanges<DBOModel>;
}