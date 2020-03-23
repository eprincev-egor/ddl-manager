import {Types} from "model-layer";
import {CommandModel} from "./CommandModel";

export class PrimaryKeyCommandModel 
extends CommandModel<PrimaryKeyCommandModel> {
    structure() {
        return {
            ...super.structure(),
            
            tableIdentify: Types.String({
                required: true
            }),
            primaryKey: Types.Array({
                element: Types.String,
                required: true,
                unique: true
            })
        };
    }
}
