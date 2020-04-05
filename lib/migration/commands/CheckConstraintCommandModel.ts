import {Types} from "model-layer";
import {CommandModel} from "./base-layers/CommandModel";
import {CheckConstraintModel} from "../../objects/CheckConstraintModel";

export class CheckConstraintCommandModel 
extends CommandModel<CheckConstraintCommandModel> {
    structure() {
        return {
            ...super.structure(),

            tableIdentify: Types.String({
                required: true
            }),
            
            constraint: Types.Model({
                Model: CheckConstraintModel,
                required: true
            })
        };
    }
}
