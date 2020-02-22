import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";
import ColumnModel from "./ColumnModel";

export default class TableModel extends BaseObjectModel<TableModel> {
    structure() {
        return {
            ...super.structure(),
            
            deprecated: Types.Boolean({
                required: true,
                default: false
            }),
            name: Types.String({
                required: true
            }),
            columns: Types.Array({
                element: ColumnModel,
                required: true
            }),
            deprecatedColumns: Types.Array({
                element: Types.String,
                unique: true,
                default: () => []
            }),
            rows: Types.Array({
                element: Types.Object
            })
        };
    }

    validate(table: this["row"]) {
        this.validateDeprecatedColumns(table);
        this.validateRows(table);
    }

    validateRows(table: this["row"]) {
        if ( !table.rows ) {
            return;
        }

        table.rows.forEach((row) => {
            const unknownColumns = [];

            for (const key in row) {
                const existsColumn = table.columns.find((column) => 
                    column.get("key") === key
                );

                if ( !existsColumn ) {
                    unknownColumns.push(key);
                }
            }

            if ( unknownColumns.length ) {
                throw new Error(`unknown row columns: ${ unknownColumns }`);
            }
        });
    }

    validateDeprecatedColumns(table: this["row"]) {
        if ( !table.deprecatedColumns || !table.columns ) {
            return;
        }

        const duplicatedKeys = table.deprecatedColumns.filter((deprecatedKey) =>
            table.columns.some((column) =>
                column.get("key") === deprecatedKey 
            )
        );

        if ( duplicatedKeys.length ) {
            throw new Error("columns should be only actual or only deprecated: " + duplicatedKeys);
        }
    }
}