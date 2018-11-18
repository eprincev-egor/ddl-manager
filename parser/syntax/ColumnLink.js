"use strict";

const ObjectLink = require("./ObjectLink");

class ColumnLink extends ObjectLink {
    clone() {
        let clone = new ColumnLink();
        this.fillClone(clone);
        return clone;
    }
}

module.exports = ColumnLink;
