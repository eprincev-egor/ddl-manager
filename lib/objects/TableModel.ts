import {Types} from "model-layer";
import {AbstractTableModel} from "./AbstractTableModel";
import {ColumnModel} from "./ColumnModel";

export class TableModel extends AbstractTableModel<TableModel> {
    structure() {
        return {
            ...super.structure(),
            
            columns: Types.Array({
                element: ColumnModel,
                required: true
            })
        };
    }

    validate(table: this["row"]) {
        this.validateDeprecatedColumns(table);
        this.validatePrimaryKey(table);
        this.validateUnique(table);
        this.validateForeignKeys(table);
    }
}