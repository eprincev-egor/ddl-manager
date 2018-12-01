"use strict";

const Syntax = require("./Syntax");

class PgReturns extends Syntax {
    is(coach) {
        return coach.isDataType() || coach.isWord();
    }

    parse(coach) {
        if ( coach.isWord("setof") ) {
            coach.expectWord("setof");
            coach.skipSpace();

            this.setof = true;
        }

        if ( coach.isWord("table") ) {
            coach.expectWord("table");
            coach.skipSpace();
            
            coach.expect("(");
            coach.skipSpace();

            this.table = coach.parseComma("PgArgument");

            coach.skipSpace();
            coach.expect(")");

        } else {
            let dataType = coach.parseDataType();
            this.type = dataType.type;
        }
    }

    clone() {
        let clone = new PgReturns();
        
        if ( this.table ) {
            clone.table = this.table.map(arg =>
                arg.clone()
            );
        } 
        else {
            clone.type = this.type;
        }

        if ( this.setof ) {
            clone.setof = true;
        }

        return clone;
    }

    toJSON() {
        let json = {};

        if ( this.table ) {
            json.table = this.table.map(arg => 
                arg.toJSON()
            );
        }
        else {
            json.type = this.type;
        }

        if ( this.setof ) {
            json.setof = true;
        }

        return json;
    }

    toString() {
        return PgReturns.returns2sql(this);
    }

    static returns2sql(returns) {
        let out = "";

        if ( returns.setof ) {
            out += "setof ";
        }

        if ( returns.table ) {
            out += `table(${ 
                returns.table.map(arg => 
                    arg.toString()
                ).join(", ") 
            })`;
        } else {
            out += returns.type;
        }

        return out;
    }
}

module.exports = PgReturns;