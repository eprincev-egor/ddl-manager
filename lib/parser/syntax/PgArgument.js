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

        if ( coach.isWord("out") ) {
            coach.expectWord("out");
            coach.skipSpace();

            this.out = true;
        }
        else if ( coach.isWord("in") ) {
            coach.expectWord("in");
            coach.skipSpace();

            this.in = true;
        }

        this.name = false;
        this.type = coach.parseDataType().type;

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

        if ( this.out ) {
            clone.out = true;
        }
        if ( this.in ) {
            clone.in = true;
        }
        
        clone.name = this.name;
        clone.type = this.type;

        if ( this.default ) {
            clone.default = this.default;
        }

        return clone;
    }

    toJSON() {
        let json = {
            name: this.name,
            type: this.type
        };

        if ( this.out ) {
            json.out = true;
        }
        if ( this.in ) {
            json.in = true;
        }

        if ( this.default ) {
            json.default = this.default;
        }

        return json;
    }

    toString() {
        return PgArgument.arg2sql(this);
    }

    static arg2sql(arg) {
        let out = "";

        if ( arg.out ) {
            out += "out ";
        }
        else if ( arg.in ) {
            out += "in ";
        }

        if ( arg.name ) {
            out += arg.name;
            out += " ";
        }

        out += arg.type;

        if ( arg.default ) {
            out += " default ";
            out += arg.default;
        }

        return out;
    }
}

module.exports = PgArgument;