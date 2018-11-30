"use strict";

const Syntax = require("./Syntax");

class PgArgument extends Syntax {
    is(coach) {
        return coach.isWord();
    }

    parse(coach, options) {
        options = options || {default: false};

        this.name = coach.readWord();
        this.name = this.name.toLowerCase();
        coach.skipSpace();

        let i = coach.i;
        try {
            let dataType = coach.parseDataType();
            this.type = dataType.type;
        } catch(err) {
            coach.i = i;
            let {schema, name} = coach.parseSchemaName();
            
            this.type = {
                schema,
                table: name
            };
        }

        if ( options.default ) {
            coach.skipSpace();

            if ( coach.isWord("default") ) {
                coach.expectWord("default");
                coach.skipSpace();

                let expression = coach.parseExpression();
                this.default = expression.toString();
            }
        }
    }

    clone() {
        let clone = new PgArgument();
        
        clone.name = this.name;
        
        if ( this.type.table ) {
            clone.type = {
                schema: this.type.schema,
                table: this.type.table
            };
        } else {
            clone.type = this.type;
        }

        if ( this.default ) {
            clone.default = this.default;
        }

        return clone;
    }

    toString() {
        let type = this.type;
        if ( type.table ) {
            type = `${type.schema}.${type.table}`;
        }

        if ( this.default ) {
            return `${this.name} ${type} default ${this.default}`;
        } else {
            return `${this.name} ${type}`;
        }
    }
}

module.exports = PgArgument;