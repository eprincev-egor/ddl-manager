import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import TriggerModel from "../../objects/TriggerModel";

export default class TriggerCommandModel extends CommandModel<TriggerCommandModel> {
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
