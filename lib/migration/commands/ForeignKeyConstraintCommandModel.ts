import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import ForeignKeyConstraintModel from "../../objects/ForeignKeyConstraintModel";

export default class ForeignKeyConstraintCommandModel 
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
