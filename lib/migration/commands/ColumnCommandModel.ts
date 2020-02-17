import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import ColumnModel from "../../objects/ColumnModel";

export default class ColumnCommandModel extends CommandModel<ColumnCommandModel> {
    structure() {
        return {
            ...super.structure(),

            tableIdentify: Types.String({
                required: true
            }),
            
            column: Types.Model({
                Model: ColumnModel,
                required: true
            })
        };
    }
}
