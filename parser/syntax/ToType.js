"use strict";

const Syntax = require("./Syntax");

// ::text

class ToType extends Syntax {
    parse(coach) {
        coach.expect("::");
        coach.skipSpace();
        this.dataType = coach.parseDataType();
        this.addChild(this.dataType);
    }
    
    is(coach, str) {
        return str[0] == ":" && str[1] == ":";
    }
    
    clone() {
        let clone = new ToType();
        clone.dataType = this.dataType.clone();
        clone.addChild(clone.dataType);
        return clone;
    }
    
    toString() {
        return "::" + this.dataType.toString();
    }
}

module.exports = ToType;
