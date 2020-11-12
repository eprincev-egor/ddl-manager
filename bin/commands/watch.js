"use strict";

const {parseConfigFromArgs} = require("../../lib/utils");
const DDLManager = require("../../lib/DDLManager");

module.exports = async function(argv) {
    let config = parseConfigFromArgs(argv);

    await DDLManager.watch({
        db: config,
        folder: config.folder
    });
};