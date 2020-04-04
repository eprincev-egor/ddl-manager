import {Types} from "model-layer";
import {CommandModel} from "./CommandModel";

export class ColumnDefaultCommandModel 
extends CommandModel<ColumnDefaultCommandModel> {
    structure() {
        return {
            ...super.structure(),

            tableIdentify: Types.String({
                required: true
            }),
            
            columnIdentify: Types.String({
                required: true
            }),

            default: Types.String({
                required: true
            })
        };
    }
}
