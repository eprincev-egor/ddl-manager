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
            let i = coach.i;
            try {
                let dataType = coach.parseDataType();
                this.type = dataType.type;
            } catch(err) {
                coach.i = i;
                let {schema, name} = coach.parseSchemaName();
                
                this.schema = schema;
                this.table = name;
            }
        }
    }

    clone() {
        let clone = new PgReturns();
        
        if ( this.schema ) {
            clone.schema = this.schema;
            clone.table = this.table;
        }
        else if ( this.table ) {
            clone.table = this.table.map(arg =>
                arg.clone()
            );
        } 
        else {
            clone.type = this.type;
        }

        return clone;
    }

    toString() {
        if ( this.schema ) {
            return `${ this.schema }.${ this.table }`;
        }
        else if ( this.table ) {
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