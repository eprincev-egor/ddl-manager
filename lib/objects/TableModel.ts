import {Types} from "model-layer";
import {AbstractTableModel} from "./AbstractTableModel";
import {ColumnModel} from "./ColumnModel";
import { IChanges } from "./base-layers/BaseDBObjectCollection";
import { ExtensionModel } from "./ExtensionModel";

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
        const fsTable = this;
        const changes: IChanges<ColumnModel> = {
            removed: [],
            created: [],
            changed: []
        };

        dbTable.row.columns.forEach((dbColumn) => {
            const key = dbColumn.get("key");
            const fsColumn = fsTable.getColumnByKey(key);

            if ( fsColumn ) {
                const hasChanges = !fsColumn.equal(dbColumn);
                if ( hasChanges ) {
                    changes.changed.push({
                        prev: dbColumn,
                        next: fsColumn
                    });
                }

                return;
            }

            changes.removed.push(dbColumn);
        });

        fsTable.row.columns.forEach((fsColumn) => {
            const key = fsColumn.get("key");
            const existsColumnInDB = !!dbTable.getColumnByKey(key);

            if ( !existsColumnInDB ) {
                changes.created.push(fsColumn);
            }
        });


        return changes;
    }
}