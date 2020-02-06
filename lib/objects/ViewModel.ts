import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";

export default class ViewModel extends BaseObjectModel<ViewModel> {
    structure() {
        return {
            schema: Types.String,
            name: Types.String
        };
    }

    getIdentify() {
        return this.row.schema + "." + this.row.name;
    }
}