import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import ViewModel from "../../objects/ViewModel";

export default class CreateViewCommandModel extends CommandModel<CreateViewCommandModel> {
    structure() {
        return {
            type: Types.String({
                required: true,
                default: "CreateView"
            }),
            view: Types.Model({
                Model: ViewModel,
                required: true
            })
        };
    }
}
