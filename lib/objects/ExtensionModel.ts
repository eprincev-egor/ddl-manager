import {Types} from "model-layer";
import {AbstractTableModel} from "./AbstractTableModel";

export class ExtensionModel extends AbstractTableModel<ExtensionModel> {
    structure() {
        return {
            ...super.structure(),
            
            forTableIdentify: Types.String({
                required: true
            })
        };
    }
}