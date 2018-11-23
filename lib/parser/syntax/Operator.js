"use strict";

const Syntax = require("./Syntax");
const CONDITION_OPERATORS = [
    "and",
    "or",
    "not",
    "isnull",
    "notnull",
    "like",
    "ilike"
];

class Operator extends Syntax {
    parse(coach) {
        let operator = "";
        
        if ( coach.isWord("operator") ) {
            coach.readWord();
            coach.skipSpace();
            coach.expect("(");
            coach.skipSpace();
            let link = coach.read(/[^)]+/);
            link = link.trim();
            
            if ( !link ) {
                coach.throwError("invalid operator");
            }
            this.operator = "operator(" + link + ")";
            coach.skipSpace();
            coach.expect(")");
            return;
        }
        
        // check condition operators
        let index = coach.i;
        let word = coach.readWord().toLowerCase();
        
        if ( CONDITION_OPERATORS.includes(word) ) {
            this.operator = word;
        }
        
        // is
        // is not
        // is not distinct from
        // is not unknown
        // is distinct from
        // is unknown
        else if ( word == "is" ) {
            this.operator = "is";
            coach.skipSpace();
            
            if ( coach.isWord("not") ) {
                coach.expectWord("not");
                coach.skipSpace();
                
                this.operator += " not";
            }
            
            if ( coach.isWord("distinct") ) {
                coach.expectWord("distinct");
                coach.skipSpace();
                
                coach.expectWord("from");
                
                this.operator += " distinct from";
            }
            else if ( coach.isWord("unknown") ) {
                coach.expectWord("unknown");
                
                this.operator += " unknown";
            }
        }
        
        // similar to
        else if ( word == "similar" ) {
            coach.skipSpace();
            coach.expectWord("to");
            
            this.operator = "similar to";
        }
        
        // another
        else {
            coach.i = index;
            
            for (; coach.i < coach.n; coach.i++) {
                let symbol = coach.str[ coach.i ];
                
                // -+ must be as two operators
                if ( symbol == "-" ) {
                    operator = "-";
                    coach.i++;
                    break;
                }
                
                if ( !/[+\-*/%~=<>!&|^]/.test(symbol) ) {
                    break;
                }
                
                operator += symbol;
            }
            
            this.operator = operator;
        }
    }
    
    is(coach, str) {
        return (
            (
                "+-*/%~=<>!&|^".indexOf(str[0]) != -1 ||
                coach.isWord("operator") ||
                coach.isWord("and") ||
                coach.isWord("or") ||
                coach.isWord("not") ||
                coach.isWord("is") ||
                coach.isWord("ilike") ||
                coach.isWord("like") ||
                coach.isWord("similar") ||
                coach.isWord("notnull") ||
                coach.isWord("isnull") 
            ) && 
            !(str[0] == "-" && str[1] == "-") && // -- comment
            !(str[0] == "/" && str[1] == "*") // /* comment
        ); 
    }
    
    clone() {
        let clone = new Operator();
        clone.operator = this.operator;
        return clone;
    }
    
    toString() {
        return this.operator;
    }
}

module.exports = Operator;
