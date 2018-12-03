"use strict";

const help = require("./help");

module.exports = function(args) {
    let commandName = args._[0];

    if ( !commandName ) {
        console.error("please set command: build or dump or watch");
    }
    else {
        console.error(`unknown command "${commandName}"`);
        console.error("please set command: build or dump or watch");
    }

    // show help info
    help(args);
};