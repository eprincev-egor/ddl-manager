import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";

export default class ForeignKeyConstraintModel 
extends BaseObjectModel<ForeignKeyConstraintModel> {
    structure() {
        return {
            ...super.structure(),

            name: Types.String({
                required: true
            }),
            columns: Types.Array({
                element: Types.String,
                required: true,
                unique: true
            }),
            referenceTableIdentify: Types.String({
                required: true
            }),
            referenceColumns: Types.Array({
                element: Types.String,
                required: true,
                unique: true
            })
        };
    }
}