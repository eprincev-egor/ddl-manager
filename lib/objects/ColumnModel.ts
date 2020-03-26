import {Types} from "model-layer";
import {BaseDBObjectModel} from "./base-layers/BaseDBObjectModel";

export class ColumnModel extends BaseDBObjectModel<ColumnModel> {
    structure() {
        return {
            ...super.structure(),

            key: Types.String({
                required: true
            }),
            type: Types.String({
                required: true
            }),
            nulls: Types.Boolean({
                required: true,
                default: true
            })
        };
    }
}