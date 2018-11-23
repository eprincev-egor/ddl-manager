"use strict";

const Syntax = require("./Syntax");

class Substring extends Syntax {
    parse(coach) {
        coach.expectWord("substring");
        
        coach.skipSpace();
        coach.expect("(");
        coach.skipSpace();

        this.str = coach.parseExpression();
        this.addChild(this.str);
        coach.skipSpace();
        
        if ( coach.isWord("from") ) {
            coach.expectWord("from");
            coach.skipSpace();
            
            this.from = coach.parseExpression();
            this.addChild(this.from);
            coach.skipSpace();
        }

        if ( coach.isWord("for") ) {
            coach.expectWord("for");
            coach.skipSpace();
            
            this.for = coach.parseExpression();
            this.addChild(this.for);
            coach.skipSpace();
        }

        coach.skipSpace();
        coach.expect(")");
        coach.skipSpace();
    }
    
    is(coach) {
        return coach.is(/^substring\s*\(/i);
    }
    
    clone() {
        let clone = new Substring();
        
        clone.str = this.str.clone();
        clone.addChild(clone.str);

        if ( this.from ) {
            clone.from = this.from.clone();
            clone.addChild(clone.from);
        }

        if ( this.for ) {
            clone.for = this.for.clone();
            clone.addChild(clone.for);
        }

        return clone;
    }
    
    toString() {
        let out = "substring(";

        out += this.str.toString();

        if ( this.from ) {
            out += " from ";
            out += this.from.toString();
        }

        if ( this.for ) {
            out += " for ";
            out += this.for.toString();
        }

        out += ")";
        return out;
    }
}

module.exports = Substring;
