"use strict";

const fs = require("fs");
const helpContent = fs.readFileSync(__dirname + "/../readme.md");

module.exports = function() {
    console.log(helpContent);
};