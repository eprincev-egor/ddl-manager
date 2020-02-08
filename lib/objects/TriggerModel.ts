import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";

export default class TriggerModel extends BaseObjectModel<TriggerModel> {
    structure() {
        return {
            schema: Types.String,
            table: Types.String,
            name: Types.String
        };
    }

    getIdentify() {
        return this.row.name + " on " + this.row.table;
    }
}