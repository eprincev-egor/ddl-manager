import { IDBO } from "../../../common";
import { Model, Types } from "model-layer";

export class FunctionDBO 
extends Model<FunctionDBO>
implements IDBO {
    structure() {
        return {
            schema: Types.String,
            name: Types.String,
            args: Types.Array({
                element: Types.String
            }),
            returns: Types.String,
            body: Types.String,
            language: Types.String,
            immutable: Types.Boolean,
            returnsNullOnNull: Types.Boolean,
            stable: Types.Boolean,
            strict: Types.Boolean,
            parallel: Types.String,
            cost: Types.Number
        };
    }

    getIdentify() {
        return `${this.row.schema}.${this.row.name}(${this.row.args})`;
    }
    
    equal(other: this) {
        return super.equal(other);
    }

    toCreateSQL() {
        const func = this.row;
        let additionalParams = "";

        additionalParams += " language ";
        additionalParams += func.language;
        
        if ( func.immutable ) {
            additionalParams += " immutable";
        }
        else if ( func.stable ) {
            additionalParams += " stable";
        }

        if ( func.returnsNullOnNull ) {
            additionalParams += " returns null on null input";
        }
        else if ( func.strict ) {
            additionalParams += " strict";
        }


        if ( func.parallel ) {
            additionalParams += " parallel ";
            additionalParams += func.parallel;
        }

        if ( func.cost != null ) {
            additionalParams += " cost " + func.cost;
        }

        let argsSql = func.args.join(",\n");

        if ( func.args.length ) {
            argsSql = "\n" + argsSql + "\n";
        }

        // отступов не должно быть!
        // иначе DDLManager.dump будет писать некрасивый код
        return `
create or replace function ${ func.schema }.${ func.name }(${argsSql}) 
returns ${ func.returns } 
${ additionalParams }
as ${ func.body };
        `.trim();
    }

    toDropSQL() {
        const row = this.row;
        return `drop function if exists ${row.schema}.${row.name}(${row.args})`;
    }
}