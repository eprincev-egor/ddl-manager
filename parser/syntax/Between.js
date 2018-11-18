"use strict";

const Syntax = require("./Syntax");

class Between extends Syntax {
    parse(coach) {
        coach.expectWord("between");
        coach.skipSpace();
        
        this.start = coach.parseExpression({ excludeOperators: ["and"] });
        this.addChild( this.start );
        
        coach.skipSpace();
        coach.expectWord("and");
        coach.skipSpace();
        
        this.end = coach.parseExpression({ excludeOperators: ["and", "or", ">", "<", ">=", "<=", "="] });
        this.addChild( this.end );
    }
    
    is(coach) {
        return coach.isWord("between");
    }
    
    clone() {
        let clone = new Between();
        
        clone.start = this.start.clone();
        clone.addChild( clone.start );
        
        clone.end = this.end.clone();
        clone.addChild( clone.end );
        
        return clone;
    }
    
    toString() {
        return `between ${ this.start } and ${ this.end }`;
    }
}

module.exports = Between;
