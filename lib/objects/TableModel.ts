import {Types} from "model-layer";
import BaseObjectModel from "./BaseDBObjectModel";
import ColumnModel from "./ColumnModel";
import CheckConstraintModel from "./CheckConstraintModel";
import UniqueConstraintModel from "./UniqueConstraintModel";
import ForeignKeyConstraintModel from "./ForeignKeyConstraintModel";

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
            }),
            primaryKey: Types.Array({
                element: Types.String,
                unique: true
            }),
            checkConstraints: Types.Array({
                element: Types.Model({
                    Model: CheckConstraintModel
                }),
                default: () => [],
                sort(
                    a: CheckConstraintModel, 
                    b: CheckConstraintModel
                ) {
                    return a.get("name") > b.get("name") ? 1 : -1;
                }
            }),
            uniqueConstraints: Types.Array({
                element: Types.Model({
                    Model: UniqueConstraintModel
                }),
                default: () => [],
                sort(
                    a: UniqueConstraintModel, 
                    b: UniqueConstraintModel
                ) {
                    return a.get("name") > b.get("name") ? 1 : -1;
                }
            }),
            foreignKeysConstraints: Types.Array({
                element: Types.Model({
                    Model: ForeignKeyConstraintModel
                }),
                default: () => [],
                sort(
                    a: ForeignKeyConstraintModel, 
                    b: ForeignKeyConstraintModel
                ) {
                    return a.get("name") > b.get("name") ? 1 : -1;
                }
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

    validatePrimaryKey(table: this["row"]) {
        if ( !table.primaryKey ) {
            return;
        }

        if ( !table.primaryKey.length ) {
            throw new Error("primary key cannot be empty array");
        }

        const unknownColumns = [];
        table.primaryKey.forEach(key => {
            const existsColumn = table.columns.find((column) => 
                column.get("key") === key
            );

            if ( !existsColumn ) {
                unknownColumns.push(key);
            }
        });

        if ( unknownColumns.length ) {
            throw new Error(`unknown primary key columns: ${ unknownColumns }`);
        }
    }

    validateUnique(table: this["row"]) {
        if ( !table.uniqueConstraints ) {
            return;
        }

        table.uniqueConstraints.forEach(uniqueConstraint => {
            const uniqueColumns = uniqueConstraint.get("unique");

            if ( !uniqueColumns.length ) {
                throw new Error(`unique constraint '${ uniqueConstraint.row.name }' cannot be empty array`);
            }

            const unknownColumns = [];
            uniqueColumns.forEach(key => {
                const existsColumn = table.columns.find((column) => 
                    column.get("key") === key
                );

                if ( !existsColumn ) {
                    unknownColumns.push(key);
                }
            });

            if ( unknownColumns.length ) {
                throw new Error(`unique constraint '${uniqueConstraint.row.name}' contain unknown columns: ${unknownColumns}`);
            }
        });
        
    }

    validateForeignKeys(table: this["row"]) {
        if ( !table.foreignKeysConstraints ) {
            return;
        }

        table.foreignKeysConstraints.forEach(foreignKeyConstraint => {
            const columns = foreignKeyConstraint.get("columns");

            if ( !columns.length ) {
                throw new Error(`columns inside foreign key constraint '${ foreignKeyConstraint.row.name }' cannot be empty array`);
            }

            const unknownColumns = [];
            columns.forEach(key => {
                const existsColumn = table.columns.find((column) => 
                    column.get("key") === key
                );

                if ( !existsColumn ) {
                    unknownColumns.push(key);
                }
            });
            if ( unknownColumns.length ) {
                throw new Error(`foreign key constraint '${ foreignKeyConstraint.row.name }' contain unknown columns: ${unknownColumns}`);
            }


            const referenceColumns = foreignKeyConstraint.get("referenceColumns");
            if ( !referenceColumns.length ) {
                throw new Error(`referenceColumns inside foreign key constraint '${ foreignKeyConstraint.row.name }' cannot be empty array`);
            }
        });
    }
}