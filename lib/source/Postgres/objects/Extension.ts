import { IExtension } from "../../../common";
import { ColumnDBO } from "./ColumnDBO";
import { Model, Types } from "model-layer";

export class Extension
extends Model<Extension>
implements IExtension {
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
            forTable: Types.String,
            name: Types.String
        };
    }

    getColumns() {
        return this.row.columns;
    }
}