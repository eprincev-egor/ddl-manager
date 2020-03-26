import {BaseValidationsController} from "./BaseValidationsController";
import {InputCommand} from "../../MigrationModel";
import { IChanges } from "../../../objects/base-layers/BaseDBObjectCollection";
import {BaseDBObjectModel} from "../../../objects/base-layers/BaseDBObjectModel";

export 
abstract class BaseStrategyController<DBOModel extends BaseDBObjectModel<any>>
extends BaseValidationsController {
    
    generate() {
        const {
            created,
            removed,
            changed
        } = this.detectChanges();
        
        removed.forEach((dbo) => {
            this.onRemove(dbo);
        });

        changed.forEach(({prev, next}) => {
            this.onChange(prev, next);
        });

        created.forEach((dbo) => {
            this.onCreate(dbo);
        });
    }

    protected onRemove(dbo: DBOModel) {
        this.drop(dbo);
    }

    protected onChange(oldDBO: DBOModel, newDBO: DBOModel) {
        this.drop(oldDBO);
        this.tryCreate(newDBO);
    }

    protected onCreate(dbo: DBOModel) {
        this.tryCreate(dbo);
    }

    protected drop(dbo: DBOModel) {
        const dropCommand = this.getDropCommand(dbo);
        this.migration.addCommand(dropCommand);
    }

    protected tryCreate(dbo: DBOModel) {
        try {
            this.validate(dbo);
            this.create(dbo);
        } catch(err) {
            if ( !this.isValidationError(err) ) {
                throw err;
            }
        }
    }

    protected create(dbo: DBOModel) {
        const createCommand = this.getCreateCommand(dbo);
        this.migration.addCommand(createCommand);
    }

    protected abstract getDropCommand(dbo: DBOModel): InputCommand;
    protected abstract getCreateCommand(dbo: DBOModel): InputCommand;
    protected abstract validate(dbo: DBOModel): void;
    protected abstract detectChanges(): IChanges<DBOModel>;
}