import {Types} from "model-layer";
import AbstractTableModel from "./AbstractTableModel";

export default class TableModel extends AbstractTableModel<TableModel> {
    structure() {
        return {
            ...super.structure(),
            
            name: Types.String({
                required: true
            })
        };
    }

    validate(table: this["row"]) {
        this.validateDeprecatedColumns(table);
        this.validateRows(table);
        this.validatePrimaryKey(table);
        this.validateUnique(table);
        this.validateForeignKeys(table);
    }
}