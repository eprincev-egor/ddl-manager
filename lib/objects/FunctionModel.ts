import BaseObjectModel from "./BaseDBObjectModel";
import { Types } from "model-layer";

export default class FunctionModel extends BaseObjectModel<FunctionModel> {
    structure() {
        return {
            ...super.structure(),
            name: Types.String,
            createdByDDLManager: Types.Boolean({
                default: true,
                required: true
            })
        }
    }
}