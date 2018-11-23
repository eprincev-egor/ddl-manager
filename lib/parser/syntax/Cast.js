"use strict";

const Syntax = require("./Syntax");

class Cast extends Syntax {
    parse(coach) {
        coach.expectWord("cast");
        coach.skipSpace();
        coach.expect("(");
        coach.skipSpace();
        
        this.expression = coach.parseExpression();
        this.addChild(this.expression);
        
        coach.skipSpace();
        coach.expectWord("as");
        coach.skipSpace();
        
        this.dataType = coach.parseDataType();
        this.addChild(this.dataType);
        
        coach.skipSpace();
        coach.expect(")");
    }
    
    is(coach) {
        return coach.isWord("cast");
    }
    
    clone() {
        let clone = new Cast();
        clone.expression = this.expression.clone();
        clone.dataType = this.dataType.clone();
        clone.addChild(clone.expression);
        clone.addChild(clone.dataType);
        return clone;
    }
    
    toString() {
        // !! .toString()
        return `cast(${ this.expression } as ${ this.dataType })`;
    }
}

module.exports = Cast;
