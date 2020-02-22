import {Types} from "model-layer";
import CommandModel from "./CommandModel";

export default class ColumnNotNullCommandModel 
extends CommandModel<ColumnNotNullCommandModel> {
    structure() {
        return {
            ...super.structure(),

            tableIdentify: Types.String({
                required: true
            }),
            
            columnIdentify: Types.String({
                required: true
            })
        };
    }
}
