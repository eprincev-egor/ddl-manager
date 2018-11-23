"use strict";

const Syntax = require("./Syntax");

class Comment extends Syntax {
    parse(coach) {
        let content = "";
        
        if ( coach.is("-") ) {
            coach.expect("--");
            
            this.isLine = true;
            
            for (; coach.i < coach.n; coach.i++) {
                let symbol = coach.str[ coach.i ];
                
                if ( /[\n\r]/.test(symbol) ) {
                    break;
                }
                
                content += symbol;
            }
        } else {
            coach.expect("/*");
            
            this.isMulti = true;
            
            for (; coach.i < coach.n; coach.i++) {
                let symbol = coach.str[ coach.i ];
                
                if ( symbol == "*" && coach.str[ coach.i + 1 ] == "/" ) {
                    coach.i += 2;
                    break;
                }
                
                content += symbol;
            }
        }
        
        this.content = content;
    }
    
    is(coach, str) {
        return (
            str[0] == "-" && str[1] == "-" ||
            str[0] == "/" && str[1] == "*"
        );
    }
    
    clone() {
        let clone = new Comment();
        clone.content = this.content;
        
        if ( this.isMulti ) {
            clone.isMulti = true;
        }
        else if ( this.isLine ) {
            clone.isLine = true;
        }
        
        return clone;
    }
    
    toString() {
        if ( this.isMulti ) {
            return "/*" + this.content + "*/";
        }
        else if ( this.isLine ) {
            return "--" + this.content;
        }
    }
}

module.exports = Comment;
