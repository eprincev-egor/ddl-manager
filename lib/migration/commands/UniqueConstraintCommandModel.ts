import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import UniqueConstraintModel from "../../objects/UniqueConstraintModel";

export default class UniqueConstraintCommandModel 
extends CommandModel<UniqueConstraintCommandModel> {
    structure() {
        return {
            ...super.structure(),
            
            tableIdentify: Types.String({
                required: true
            }),
            
            constraint: Types.Model({
                Model: UniqueConstraintModel,
                required: true
            })
        };
    }
}
