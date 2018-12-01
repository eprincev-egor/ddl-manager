"use strict";

const Syntax = require("./Syntax");

class PgArgument extends Syntax {
    is(coach) {
        return coach.isWord();
    }

    parse(coach, options) {
        options = options || {default: false};

        if ( coach.is(/\w+\s+\w+/) ) {
            // func(id integer, name text)
            this.name = coach.readWord();
            this.name = this.name.toLowerCase();
            coach.skipSpace();
        }
        else {
            // func(text, text)
            this.name = false;
        }

        let dataType = coach.parseDataType();
        this.type = dataType.type;

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