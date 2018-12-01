"use strict";

const Syntax = require("./Syntax");

class PgArgument extends Syntax {
    is(coach) {
        return coach.isWord();
    }

    // func(id integer, name text)
    // or
    // func(integer, text)
    parse(coach, options) {
        options = options || {default: false};

        this.name = false;
        this.type = coach.parseDataType().type;

        console.log(this.type);
        if ( coach.is(/\s*\w+/i) ) {
            this.name = this.type;
            
            // if dataType unknown type
            // then he returns public.type
            this.name = this.name.replace(/^public\./, "");
            
            coach.skipSpace();
            this.type = coach.parseDataType().type;
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
        clone.type = this.type;

        if ( this.default ) {
            clone.default = this.default;
        }

        return clone;
    }

    toString() {
        let out = "";

        if ( this.name ) {
            out += this.name;
            out += " ";
        }

        out += this.type;

        if ( this.default ) {
            out += " default ";
            out += this.default;
        }

        return out;
    }
}

module.exports = PgArgument;