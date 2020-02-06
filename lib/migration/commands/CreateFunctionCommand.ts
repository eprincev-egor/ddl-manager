import {Types} from "model-layer";
import CommandModel from "./CommandModel";

export default class CreateFunctionCommand extends CommandModel<CreateFunctionCommand> {
    structure() {
        return {
            type: Types.String({
                required: true,
                default: "CreateFunction"
            })
        };
    }
}
