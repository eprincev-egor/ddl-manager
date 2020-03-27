import {SimpleMigrator} from "./base-layers/SimpleMigrator";
import { NameValidator } from "./validators/NameValidator";
import { TriggerModel } from "../../objects/TriggerModel";
import { TriggerCommandModel } from "../commands/TriggerCommandModel";
import { TriggerValidator } from "./validators/TriggerValidator";

export class TriggersMigrator
extends SimpleMigrator<TriggerModel> {

    protected calcChanges() {
        const fsTriggers = this.fs.row.triggers;
        const dbTriggers = this.db.row.triggers;
        const changes = fsTriggers.compareWithDB(dbTriggers);
        return changes;
    }

    protected getValidators() {
        return [
            NameValidator,
            TriggerValidator
        ];
    }

    protected createDropCommand(triggerModel: TriggerModel) {
        return new TriggerCommandModel({
            type: "drop",
            trigger: triggerModel
        });
    }

    protected createCreateCommand(triggerModel: TriggerModel) {
        return new TriggerCommandModel({
            type: "create",
            trigger: triggerModel
        });
    }

}