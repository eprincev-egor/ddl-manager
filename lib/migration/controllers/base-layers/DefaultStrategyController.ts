import ValidationsController from "./ValidationsController";
import {InputCommand, InputError} from "../../MigrationModel";
import { IChanges } from "../../../objects/BaseDBObjectCollection";
import NamedAndMovableDBOModel from "../../../objects/NamedAndMovableDBOModel";

export default 
abstract class DefaultStrategyController
extends ValidationsController {
    
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

    drop(dbo: NamedAndMovableDBOModel<any>) {
        const dropCommand = this.getDropCommand(dbo);
        this.migration.addCommand(dropCommand);
    }

    create(dbo: NamedAndMovableDBOModel<any>) {
        const createCommand = this.getCreateCommand(dbo);
        this.migration.addCommand(createCommand);
    }

    saveErrors(errors: InputError[]) {
        for (const err of errors) {
            this.migration.addError(err);
        }
    }

    abstract getDropCommand(dbo: NamedAndMovableDBOModel<any>): InputCommand;
    abstract getCreateCommand(dbo: NamedAndMovableDBOModel<any>): InputCommand;
    abstract validate(dbo: NamedAndMovableDBOModel<any>): (InputError | undefined)[];
    abstract detectChanges(): IChanges<NamedAndMovableDBOModel<any>>;
}