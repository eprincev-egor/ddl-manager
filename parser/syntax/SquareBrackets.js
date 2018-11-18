"use strict";

const Syntax = require("./Syntax");

class SquareBrackets extends Syntax {
    parse(coach) {
        coach.expect("[");
        coach.skipSpace();
        
        this.content = coach.parseExpression();
        this.addChild( this.content );

        coach.skipSpace();
        coach.expect("]");
    }
    
    is(coach) {
        return coach.is("[");
    }
    
    clone() {
        let clone = new SquareBrackets();

        clone.content = this.content.clone();
        clone.addChild(clone.content);

        return clone;
    }
    
    toString() {
        return `[${ this.content.toString() }]`;
    }
}

module.exports = SquareBrackets;
