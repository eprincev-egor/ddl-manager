"use strict";

const ObjectLink = require("./ObjectLink");

class FunctionLink extends ObjectLink {
    clone() {
        let clone = new FunctionLink();
        this.fillClone(clone);
        return clone;
    }
}

module.exports = FunctionLink;
