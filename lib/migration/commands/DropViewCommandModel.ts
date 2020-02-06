import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import ViewModel from "../../objects/ViewModel";

export default class DropViewCommandModel extends CommandModel<DropViewCommandModel> {
    structure() {
        return {
            type: Types.String({
                required: true,
                default: "DropView"
            }),
            view: Types.Model({
                Model: ViewModel,
                required: true
            })
        };
    }
}
