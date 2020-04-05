import {Types} from "model-layer";
import {CommandModel} from "./base-layers/CommandModel";
import {FunctionModel} from "../../objects/FunctionModel";

export class FunctionCommandModel extends CommandModel<FunctionCommandModel> {
    structure() {
        return {
            ...super.structure(),
            
            function: Types.Model({
                Model: FunctionModel,
                required: true
            })
        };
    }
}
