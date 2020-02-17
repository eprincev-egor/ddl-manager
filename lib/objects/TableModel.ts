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
            }),
            deprecatedColumns: Types.Array({
                element: Types.String,
                unique: true,
                default: () => []
            })
        };
    }

    validate(row: this["row"]) {
        if ( row.deprecatedColumns && row.columns ) {
            const duplicatedKeys = row.deprecatedColumns.filter((deprecatedKey) =>
                row.columns.some((column) =>
                    column.get("key") === deprecatedKey 
                )
            );

            if ( duplicatedKeys.length ) {
                throw new Error("columns should be only actual or only deprecated: " + duplicatedKeys);
            }
        }
    }
}