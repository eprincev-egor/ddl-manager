import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import TableModel from "../../objects/TableModel";

export default class RowsCommandModel 
extends CommandModel<RowsCommandModel> {
    structure() {
        return {
            ...super.structure(),
            
            table: Types.Model({
                Model: TableModel,
                required: true
            }),
            values: Types.Array({
                element: Types.Array({
                    element: Types.String
                }),
                required: true
            })
        };
    }
}
