"use strict";

const Syntax = require("./Syntax");
const extractFields = [
    "century",
    "day",
    "decade",
    "dow",
    "doy",
    "epoch",
    "hour",
    "microseconds",
    "millennium",
    "milliseconds",
    "minute",
    "month",
    "quarter",
    "second",
    "timezone",
    "timezone_hour",
    "timezone_minute",
    "week",
    "year"
];

class Extract extends Syntax {
    parse(coach) {
        coach.expectWord("extract");
        coach.skipSpace();
        coach.expect("(");
        coach.skipSpace();
        
        this.field = coach.readWord();
        this.field = this.field.toLowerCase();
        coach.skipSpace();
        
        if ( !extractFields.includes(this.field) ) {
            coach.throwError("unrecognized extract field: " + this.field);
        }

        coach.skipSpace();
        coach.expectWord("from");
        coach.skipSpace();
        
        if ( coach.isDataType() ) {
            this.type = coach.parseDataType();
            coach.skipSpace();
        }

        this.source = coach.parseExpression();
        this.addChild(this.source);
        
        coach.skipSpace();
        coach.expect(")");
    }
    
    is(coach) {
        return coach.isWord("extract");
    }
    
    clone() {
        let clone = new Extract();

        clone.field = this.field;

        if ( this.type ) {
            clone.type = this.type.clone();
        }

        clone.source = this.source.clone();
        clone.addChild(clone.source);
        
        return clone;
    }
    
    toString() {
        let out = `extract( ${this.field} from `;

        if ( this.type ) {
            out += this.type.toString();
            out += " ";
        }

        out += this.source.toString();

        out += ")";
        return out;
    }
}

module.exports = Extract;
