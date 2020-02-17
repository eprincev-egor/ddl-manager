import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";

export default class ViewModel extends BaseObjectModel<ViewModel> {
    structure() {
        return {
            ...super.structure(),
            name: Types.String
        };
    }
}