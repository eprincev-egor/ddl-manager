"use strict";

const DoubleQuotes = require("./DoubleQuotes");

class ObjectName extends DoubleQuotes {
    parse(coach) {
        if ( coach.isDoubleQuotes() ) {
            super.parse(coach);
        }
        else {
            this.word = coach.expectWord();
        }
    }

    is(coach) {
        return coach.isDoubleQuotes() || coach.isWord();
    }

    clone() {
        let clone = new ObjectName();
        this.fillClone( clone );
        return clone;
    }

    fillClone(clone) {
        if ( this.word ) {
            clone.word = this.word;
        } else {
            super.fillClone( clone );
        }
    }

    toString() {
        if ( this.word ) {
            return this.word;
        } else {
            return super.toString();
        }
    }
    
    toLowerCase() {
        if ( this.word ) {
            return this.word.toLowerCase();
        } else {
            return this.content;
        }
    }

    equal(anotherObject) {
        if ( this.word && anotherObject.word ) {
            return this.word.toLowerCase() == anotherObject.word.toLowerCase();
        }
        else if ( this.word && anotherObject.content ) {
            return this.word == anotherObject.content;
        }
        else if ( this.content && anotherObject.word ) {
            return this.content == anotherObject.word;
        }
        else {
            return this.content == anotherObject.content;
        }
    }

    equalString(string) {
        if ( this.word ) {
            return this.word.toLowerCase() == string.toLowerCase();
        } else {
            return this.content == string;
        }
    }

    strictEqualString(string) {
        if ( this.word ) {
            return this.word == string;
        } else {
            return this.content == string;
        }
    }
}

module.exports = ObjectName;
