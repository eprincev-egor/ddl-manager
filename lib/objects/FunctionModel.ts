import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";

export default class FunctionModel extends BaseObjectModel<FunctionModel> {
    structure() {
        return {
            schema: Types.String,
            name: Types.String,
            args: Types.String({
                default: ""
            })
        };
    }

    getIdentify() {
        return `${this.row.schema}.${this.row.name}(${this.row.args})`;
    }
}