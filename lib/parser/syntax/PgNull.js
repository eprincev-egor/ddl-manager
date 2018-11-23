"use strict";

const Syntax = require("./Syntax");

class PgNull extends Syntax {
    parse(coach) {
        coach.expectWord("null");
        this.null = true;
    }
    
    is(coach) {
        return coach.isWord("null");
    }
    
    clone() {
        return new PgNull();
    }
    
    toString() {
        return "null";
    }
}

module.exports = PgNull;
