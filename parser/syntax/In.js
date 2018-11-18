"use strict";

const Syntax = require("./Syntax");

class In extends Syntax {
    parse(coach) {
        coach.expectWord("in");
        coach.skipSpace();
        coach.expect("(");
        coach.skipSpace();
        
        this.in = coach.parseComma("Expression");
        this.in.forEach(expression => this.addChild(expression));
        
        
        coach.skipSpace();
        coach.expect(")");
    }
    
    is(coach) {
        return coach.isWord("in");
    }
    
    clone() {
        let clone = new In();
        
        clone.in = this.in.map(expression => {
            let expClone = expression.clone();
            clone.addChild(expClone);
            return expClone;
        });
        
        return clone;
    }
    
    toString() {
        return `in (${ this.in.join(", ") })`;
    }
}

module.exports = In;
