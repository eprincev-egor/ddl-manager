#!/usr/bin/env node
"use strict";

const parseArgs = require("minimist");

const args = parseArgs(
    process.argv[0] == "ddl-manager" ?
        process.argv.slice(1) :
        process.argv.slice(2)
);

const commands = {
    undefined: require("./commands/undefined"),
    help: require("./commands/help"),
    build: require("./commands/build"),
    watch: require("./commands/watch"),
    dump: require("./commands/dump")
};

// $ ddl-manager build
// commandName will "build"
const commandName = args._[0];
const command = commands[ commandName ] || commands.undefined;

// run command
(async function() {
    try {
        
        await command(args);

    } catch(err) {
        console.error("Error: " + err.message);
        process.exit(1);
    }
})();

