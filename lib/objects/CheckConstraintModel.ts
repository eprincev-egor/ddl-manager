import {Types} from "model-layer";
import {BaseDBObjectModel} from "./base-layers/BaseDBObjectModel";

export class CheckConstraintModel 
extends BaseDBObjectModel<CheckConstraintModel> {
    structure() {
        return {
            ...super.structure(),

            name: Types.String({
                required: true
            })
        };
    }
}