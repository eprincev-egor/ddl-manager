import {Types} from "model-layer";
import {AbstractTableModel} from "./AbstractTableModel";
import {ColumnModel} from "./ColumnModel";
import { ExtensionModel } from "./ExtensionModel";
import { IChanges, Changes } from "../state/Changes";
import { BaseDBObjectModel } from "./base-layers/BaseDBObjectModel";
import { UniqueConstraintModel } from "./UniqueConstraintModel";

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
            const extensionColumns = extension.get("columns");
            if ( extensionColumns ) {
                extensionColumns.forEach(column => {
                    const key = column.get("key");
                    cloneTable.setColumn(key, column);
                });
            }

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

            const extensionForeignKeys = extension.get("foreignKeysConstraints");
            if ( extensionForeignKeys ) {
                const tableForeignKeys = cloneTable.get("foreignKeysConstraints");
                cloneTable.set({
                    foreignKeysConstraints: [
                        ...tableForeignKeys,
                        ...extensionForeignKeys
                    ]
                });
            }

            const extensionCheckConstraints = extension.get("checkConstraints");
            if ( extensionCheckConstraints ) {
                const tableCheckConstraints = cloneTable.get("checkConstraints");
                cloneTable.set({
                    checkConstraints: [
                        ...tableCheckConstraints,
                        ...extensionCheckConstraints
                    ]
                });
            }

        }

        return cloneTable;
    }

    addUniqueConstraint(newUniqueConstraint: UniqueConstraintModel) {
        const thisUniqueConstraints = this.row.uniqueConstraints;
        
        this.set({
            uniqueConstraints: [
                ...thisUniqueConstraints,
                newUniqueConstraint
            ]
        });
    }

    setColumn(key: string, newColumn: ColumnModel) {
        const thisColumns = this.row.columns;
        const newColumns = [...thisColumns];

        const oldColumnIndex = newColumns.findIndex((column) => 
            column.get("key") === key
        );
        if ( oldColumnIndex !== -1 ) {
            newColumns.splice(oldColumnIndex, 1);
        }

        newColumns.push(newColumn);
        
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