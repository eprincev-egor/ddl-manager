import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";

export default class ColumnModel extends BaseObjectModel<ColumnModel> {
    structure() {
        return {
            key: Types.String({
                required: true
            }),
            type: Types.String({
                required: true
            })
        };
    }

    getIdentify() {
        return this.row.key;
    }
}