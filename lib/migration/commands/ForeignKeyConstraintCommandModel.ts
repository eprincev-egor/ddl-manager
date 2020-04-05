import {Types} from "model-layer";
import {CommandModel} from "./base-layers/CommandModel";
import {ForeignKeyConstraintModel} from "../../objects/ForeignKeyConstraintModel";

export class ForeignKeyConstraintCommandModel 
extends CommandModel<ForeignKeyConstraintCommandModel> {
    structure() {
        return {
            ...super.structure(),
            
            tableIdentify: Types.String({
                required: true
            }),
            
            foreignKey: Types.Model({
                Model: ForeignKeyConstraintModel,
                required: true
            })
        };
    }
}
