import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";
import ColumnModel from "./ColumnModel";

export default class TableModel extends BaseObjectModel<TableModel> {
    structure() {
        return {
            schema: Types.String,
            name: Types.String,
            columns: Types.Array({
                element: ColumnModel
            })
        };
    }

    getIdentify() {
        return `${this.row.schema}.${this.row.name}`;
    }
}