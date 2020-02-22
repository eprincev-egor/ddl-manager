import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import TableModel from "../../objects/TableModel";

export default class CreateRowsCommandModel 
extends CommandModel<CreateRowsCommandModel> {
    structure() {
        return {
            ...super.structure(),
            
            table: Types.Model({
                Model: TableModel,
                required: true
            }),
            rows: Types.Array({
                element: Types.Object,
                required: true
            })
        };
    }
}
