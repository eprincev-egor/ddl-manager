"use strict";

const Syntax = require("./Syntax");

class PgReturns extends Syntax {
    is(coach) {
        return coach.isDataType() || coach.isWord("table");
    }

    parse(coach) {
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
        } else {
            clone.type = this.type;
        }

        return clone;
    }

    toString() {
        if ( this.table ) {
            return `table(${ 
                this.table.map(arg => 
                    arg.toString()
                ).join(", ") 
            })`;
        } else {
            return this.type;
        }
    }
}

module.exports = PgReturns;