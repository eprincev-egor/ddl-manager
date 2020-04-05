import {Types} from "model-layer";
import {CommandModel} from "./base-layers/CommandModel";
import {TriggerModel} from "../../objects/TriggerModel";

export class TriggerCommandModel extends CommandModel<TriggerCommandModel> {
    structure() {
        return {
            ...super.structure(),
            
            trigger: Types.Model({
                Model: TriggerModel,
                required: true
            })
        };
    }
}
