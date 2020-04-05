import {Types} from "model-layer";
import {CommandModel} from "./base-layers/CommandModel";
import {ViewModel} from "../../objects/ViewModel";

export class ViewCommandModel extends CommandModel<ViewCommandModel> {
    structure() {
        return {
            ...super.structure(),
            
            view: Types.Model({
                Model: ViewModel,
                required: true
            })
        };
    }
}
