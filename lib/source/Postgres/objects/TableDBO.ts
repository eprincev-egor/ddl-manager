import { ITableDBO } from "../../../common";
import { ColumnDBO } from "./ColumnDBO";
import { Model, Types } from "model-layer";

export class TableDBO
extends Model<TableDBO>
implements ITableDBO {
    structure() {
        return {
            deprecated: Types.Boolean({
                required: true,
                default: false
            }),
            deprecatedColumns: Types.Array({
                element: Types.String,
                nullAsEmpty: true
            }),
            columns: Types.Array({
                element: ColumnDBO,
                nullAsEmpty: true
            }),
            constraints: Types.Array({
                element: Types.Any,
                nullAsEmpty: true
            }),
            values: Types.Array({
                element: Types.Array({
                    element: Types.String
                }),
                nullAsEmpty: true
            }),
            schema: Types.String,
            name: Types.String,
            inherits: Types.Array({
                element: Types.String,
                nullAsEmpty: true
            })
        };
    }

    getIdentify() {
        return `${this.row.schema}.${this.row.name}`;
    }

    equal(other: this) {
        return super.equal(other);
    }

    getColumns() {
        return this.row.columns;
    }

    toCreateSQL() {
        const columnsSQL = this.row.columns.map(column => 
            column.toTableBodySQL()
        ).join(",");

        let inherits = "";
        if ( this.row.inherits ) {
            inherits += `inherits( ${ this.row.inherits } )`;
        }
        
        return `
            create table ${this.row.schema}.${this.row.name} (
                ${columnsSQL}
            )
            ${inherits}
        `;
    }

    toDropSQL() {
        return `drop table ${this.row.schema}.${this.row.name}`;
    }
}