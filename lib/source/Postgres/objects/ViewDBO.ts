import { IDBO } from "../../../common";
import { Model, Types } from "model-layer";

export class ViewDBO 
extends Model<ViewDBO>
implements IDBO {
    structure() {
        return {
            schema: Types.String,
            name: Types.String,
            select: Types.String
        };
    }

    getIdentify() {
        return `${this.row.schema}.${this.row.name}`;
    }

    toCreateSQL() {
        const row = this.row;
        let out = "view ";
        
        if ( row.schema ) {
            out += row.schema;
            out += ".";
        }
        out += row.name;

        out += " as ";
        out += row.select;

        return out;
    }

    toDropSQL() {
        const row = this.row;
        return `drop view if exists ${row.schema}.${row.name}`;
    }
}