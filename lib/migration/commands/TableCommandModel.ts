import {Types} from "model-layer";
import {CommandModel} from "./CommandModel";
import {TableModel} from "../../objects/TableModel";

export class TableCommandModel extends CommandModel<TableCommandModel> {
    structure() {
        return {
            ...super.structure(),
            
            table: Types.Model({
                Model: TableModel,
                required: true
            })
        };
    }
}
