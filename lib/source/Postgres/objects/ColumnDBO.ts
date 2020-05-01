import { IColumnDBO } from "../../../common";
import { Model, Types } from "model-layer";

export class ColumnDBO
extends Model<ColumnDBO>
implements IColumnDBO {
    structure() {
        return {
            table: Types.String,
            name: Types.String,
            type: Types.String,
            nulls: Types.Boolean({
                default: true
            }),
            default: Types.String
        };
    }

    getIdentify() {
        return `${this.row.table}.${this.row.name}`;
    }

    equal(other: this) {
        return super.equal(other);
    }

    getDefaultSQL() {
        return this.row.default;
    }

    getNulls() {
        return this.row.nulls;
    }

    getTypeSQL() {
        return this.row.type;
    }

    toCreateSQL() {
        return `
            alter table ${this.row.table}
            add column ${this.toTableBodySQL()}
        `;
    }

    toTableBodySQL() {
        const row = this.row;
        return `
            ${row.name} ${row.type} 
            default ${row.default} 
            ${row.nulls ? "" : "not null"}
        `;
    }

    toDropSQL() {
        return `alter table ${this.row.table} drop column ${this.row.name}`;
    }
}