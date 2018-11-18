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
        if ( this.default ) {
            return `${this.name} ${this.type} default ${this.default}`;
        } else {
            return `${this.name} ${this.type}`;
        }
    }
}

module.exports = PgArgument;