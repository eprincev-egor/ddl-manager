import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";

export default class CheckConstraintModel 
extends BaseObjectModel<CheckConstraintModel> {
    structure() {
        return {
            ...super.structure(),

            name: Types.String({
                required: true
            })
        };
    }
}