"use strict";
const Syntax = require("./Syntax");

// 42
// 3.5
// 4.
// .001
// 5e2
// 1.925e-3

// https://www.postgresql.org/docs/9.1/static/datatype-numeric.html
const MAX_INTEGER_SIZE = 2147483647;
const MIN_INTEGER_SIZE = -2147483647;

class PgNumber extends Syntax {
    parse(coach) {
        let intPart = "",
            floatPart = "",
            ePart = "";
        
        intPart = this.readDigits( coach );
        if ( coach.is(".") ) {
            coach.i++;
            floatPart = this.readDigits( coach );
        }
        
        if ( coach.is("e") || coach.is("E") ) {
            coach.i++;
            
            if ( coach.is("+") || coach.is("-") ) {
                ePart = coach.str[ coach.i ];
                coach.i++;
            }
            
            ePart += this.readDigits( coach );
        }
        
        
        if ( floatPart ) {
            this.number = intPart + "." + floatPart;
        } else {
            this.number = intPart;
        }
        
        if ( ePart ) {
            this.number += "e" + ePart;
        }
    }
    
    readDigits(coach) {
        let digits = "";
        for (; coach.i < coach.n; coach.i++) {
            let symbol = coach.str[ coach.i ];
            
            if ( /\d/.test(symbol) ) {
                digits += symbol;
            } else {
                break;
            }
        }
        return digits;
    }
    
    is(coach, str) {
        return str.search(/[\d.]/) === 0;
    }
    
    clone() {
        let clone = new PgNumber();
        clone.number = this.number;
        return clone;
    }
    
    toString() {
        return this.number;
    }
    
    // 1.1 => numeric
    // 1 => integer
    getType() {
        if ( /\./.test(this.number) ) {
            return "numeric";
        }
        
        let numb = +this.number;
        if ( 
            numb === numb && // not NaN
            numb < MAX_INTEGER_SIZE && 
            numb > MIN_INTEGER_SIZE 
        ) {
            return "integer";
        }
        
        return "bigint";
    }
}

module.exports = PgNumber;
