import {Types} from "model-layer";
import {AbstractTableModel} from "./AbstractTableModel";
import {ColumnModel} from "./ColumnModel";
import { ExtensionModel } from "./ExtensionModel";
import { IChanges, Changes } from "../state/Changes";
import { BaseDBObjectModel } from "./base-layers/BaseDBObjectModel";

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

    isDeprecatedColumn(key: string) {
        return this.row.deprecatedColumns.includes(key);
    }

    concatWithExtensions(extensions: ExtensionModel[]): this {
        const cloneTable = this.clone();

        for (const extension of extensions) {
            extension.get("columns").forEach(column => {
                cloneTable.addColumn(column);
            });

            const extensionValues = extension.get("values");
            if ( extensionValues ) {
                cloneTable.set({
                    values: extensionValues
                });
            }

            const extensionUniqueConstraints = extension.get("uniqueConstraints");
            if ( extensionUniqueConstraints ) {
                const tableUniqueConstraints = cloneTable.get("uniqueConstraints");
                cloneTable.set({
                    uniqueConstraints: [
                        ...tableUniqueConstraints,
                        ...extensionUniqueConstraints
                    ]
                });
            }
        }

        return cloneTable;
    }

    addColumn(newColumn: ColumnModel) {
        const thisColumns = this.row.columns;
        const newColumns = [...thisColumns, newColumn];
        this.set({
            columns: newColumns
        });
    }

    compareColumnsWithDBTable(dbTable: TableModel): IChanges<ColumnModel> {
        const fsColumns = this.get("columns");
        const dbColumns = dbTable.get("columns");

        const changes = new Changes<ColumnModel>();
        changes.detect(fsColumns, dbColumns);
        
        return changes;
    }

    compareConstraintsWithDBTable<TConstraintModel extends BaseDBObjectModel<any>>(
        key: keyof this["row"],
        dbTable: this
    ): IChanges<TConstraintModel> {
        
        const fsConstraints = this.get(key) as any;
        const dbConstraints = dbTable.get(key) as any;

        const changes = new Changes<TConstraintModel>();
        changes.detect(fsConstraints, dbConstraints);
        
        return changes;
    }
}