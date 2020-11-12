"use strict";

const {parseConfigFromArgs} = require("../../lib/utils");
const DDLManager = require("../../lib/DDLManager");

module.exports = async function(argv) {
    let config = parseConfigFromArgs(argv);

    await DDLManager.dump({
        db: config,
        folder: config.folder,
        unfreeze: config.unfreeze
    });
};