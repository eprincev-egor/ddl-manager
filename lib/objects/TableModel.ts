import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";
import ColumnModel from "./ColumnModel";

export default class TableModel extends BaseObjectModel<TableModel> {
    structure() {
        return {
            ...super.structure(),
            
            name: Types.String,
            columns: Types.Array({
                element: ColumnModel
            })
        };
    }
}