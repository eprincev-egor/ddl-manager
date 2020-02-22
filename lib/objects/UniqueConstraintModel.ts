import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";

export default class UniqueConstraintModel 
extends BaseObjectModel<UniqueConstraintModel> {
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