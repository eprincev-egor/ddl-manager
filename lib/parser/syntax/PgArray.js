"use strict";

const Syntax = require("./Syntax");

class PgArray extends Syntax {
    parse(coach) {
        coach.expectWord("array");
        
        coach.skipSpace();
        coach.expect("[");
        coach.skipSpace();

        if ( !coach.is("]") ) {
            this.items = coach.parseComma("Expression");
            this.items.forEach(item => this.addChild(item));
        } else {
            this.items = [];
        }

        coach.skipSpace();
        coach.expect("]");
    }
    
    is(coach) {
        return coach.isWord("array");
    }
    
    clone() {
        let clone = new PgArray();
        
        clone.items = this.items.map(item => item.clone());
        clone.items.forEach(item => clone.addChild(item));

        return clone;
    }
    
    toString() {
        let out = "array[";
        
        out += this.items.map(item => item.toString()).join(", ");

        out += "]";
        return out;
    }
    
    getType() {
        return "unknown";
    }
}

module.exports = PgArray;

