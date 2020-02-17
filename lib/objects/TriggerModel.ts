import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";

export default class TriggerModel extends BaseObjectModel<TriggerModel> {
    structure() {
        return {
            ...super.structure(),
            
            tableIdentify: Types.String({
                required: true
            }),
            functionIdentify: Types.String({
                required: true
            }),
            name: Types.String({
                required: true
            })
        };
    }
}