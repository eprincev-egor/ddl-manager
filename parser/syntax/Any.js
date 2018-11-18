"use strict";

const Syntax = require("./Syntax");

class Any extends Syntax {
    parse(coach) {
        if ( coach.isWord("any") ) {
            coach.expectWord("any");
            this.type = "any";
        }
        else if ( coach.isWord("all") ) {
            coach.expectWord("all");
            this.type = "all";
        }
        else {
            coach.expectWord("some");
            this.type = "some";
        }

        coach.skipSpace();
        coach.expect("(");
        coach.skipSpace();
        
        // ту мог быть select, но мы используем any только для default в аргументах функции
        // а его там не может быть
        this.array = coach.parseExpression();
        this.addChild( this.array );
        
        coach.skipSpace();
        coach.expect(")");
    }
    
    is(coach) {
        let isKeyword = (
            coach.isWord("any") ||
            coach.isWord("all") ||
            coach.isWord("some")
        );
            
        if ( !isKeyword ) {
            return false;
        }

        let i = coach.i;
        coach.readWord();
        coach.skipSpace();

        let isBracket = coach.is("(");
        coach.i = i;
        
        return isKeyword && isBracket;
    }
    
    clone() {
        let clone = new Any();

        clone.type = this.type;
        
        clone.array = this.array.clone();
        clone.addChild(clone.array);
        
        return clone;
    }
    
    toString() {
        return `${this.type} (${ this.array })`;
    }
}

module.exports = Any;
