import {Types} from "model-layer";
import AbstractTableModel from "./AbstractTableModel";

export default class ExtensionModel extends AbstractTableModel<ExtensionModel> {
    structure() {
        return {
            ...super.structure(),
            
            name: Types.String({
                required: true
            }),
            forTableIdentify: Types.String({
                required: true
            })
        };
    }
}