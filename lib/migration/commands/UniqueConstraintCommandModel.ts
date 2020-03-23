import {Types} from "model-layer";
import {CommandModel} from "./CommandModel";
import {UniqueConstraintModel} from "../../objects/UniqueConstraintModel";

export class UniqueConstraintCommandModel 
extends CommandModel<UniqueConstraintCommandModel> {
    structure() {
        return {
            ...super.structure(),
            
            tableIdentify: Types.String({
                required: true
            }),
            
            unique: Types.Model({
                Model: UniqueConstraintModel,
                required: true
            })
        };
    }
}
