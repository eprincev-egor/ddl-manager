import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import FunctionModel from "../../objects/FunctionModel";

export default class DropFunctionCommandModel extends CommandModel<DropFunctionCommandModel> {
    structure() {
        return {
            type: Types.String({
                required: true,
                default: "DropFunction"
            }),
            function: Types.Model({
                Model: FunctionModel,
                required: true
            })
        };
    }
}
