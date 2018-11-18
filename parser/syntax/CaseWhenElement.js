"use strict";

const Syntax = require("./Syntax");

class CaseWhenElement extends Syntax {
    parse(coach) {
        coach.expectWord("when");
        coach.skipSpace();
        
        this.when = coach.parseExpression();
        this.addChild(this.when);
        coach.skipSpace();
        
        coach.expectWord("then");
        coach.skipSpace();
        
        this.then = coach.parseExpression();
        this.addChild(this.then);
    }
    
    is(coach) {
        return coach.isWord("when");
    }
    
    clone() {
        let clone = new CaseWhenElement();
        clone.when = this.when.clone();
        clone.then = this.then.clone();
        clone.addChild(clone.when);
        clone.addChild(clone.then);
        return clone;
    }
    
    toString() {
        // !! .toString()
        return `when ${ this.when } then ${ this.then }`;
    }
}

module.exports = CaseWhenElement;
