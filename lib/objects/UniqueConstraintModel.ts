import {Types} from "model-layer";
import {BaseDBObjectModel} from "./base-layers/BaseDBObjectModel";

export class UniqueConstraintModel 
extends BaseDBObjectModel<UniqueConstraintModel> {
    structure() {
        return {
            ...super.structure(),

            name: Types.String({
                required: true
            }),
            unique: Types.Array({
                element: Types.String,
                required: true,
                unique: true
            })
        };
    }
}