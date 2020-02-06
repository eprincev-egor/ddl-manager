import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import {FunctionModel} from "../../Functions";

export default class CreateFunctionCommandModel extends CommandModel<CreateFunctionCommandModel> {
    structure() {
        return {
            type: Types.String({
                required: true,
                default: "CreateFunction"
            }),
            function: Types.Model({
                Model: FunctionModel,
                required: true
            })
        };
    }
}
