import { IDBO } from "../../../common";
import { Model, Types } from "model-layer";

export class TriggerDBO 
extends Model<TriggerDBO>
implements IDBO {
    structure() {
        return {
            name: Types.String,
            events: Types.String,
            table: Types.String,
            procedure: Types.String,

            constraint: Types.Boolean,
            deferrable: Types.Boolean,
            statement: Types.Boolean,
            initially: Types.String,
            when: Types.String
        };
    }

    getIdentify() {
        return `trigger ${this.row.name} on ${this.row.table}`;
    }
    
    equal(other: this) {
        return super.equal(other);
    }

    toCreateSQL() {
        const trigger = this.row;
        let out = "create ";

        if ( trigger.constraint ) {
            out += "constraint ";
        }
        
        out += `trigger ${trigger.name}\n`;

        // after|before insert|update|delete
        out += trigger.events;
        out += " ";

        // on table
        out += `\non ${trigger.table}`;

        if ( trigger.deferrable === true ) {
            out += " deferrable";
        }
        else if ( trigger.deferrable === false ) {
            out += " not deferrable";
        }

        if ( trigger.initially ) {
            out += " initially ";
            out += trigger.initially;
        }


        if ( trigger.statement ) {
            out += "\nfor each statement ";
        } else {
            out += "\nfor each row ";
        }

        if ( trigger.when ) {
            out += "\nwhen ( ";
            out += trigger.when;
            out += " ) ";
        }

        out += `\nexecute procedure ${ trigger.procedure };`;

        return out;
    }

    toDropSQL() {
        const trigger = this.row;
        return `drop trigger if exists ${trigger.name} on ${trigger.table}`;
    }
}