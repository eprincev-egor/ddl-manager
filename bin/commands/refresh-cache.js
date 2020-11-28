"use strict";

const {parseConfigFromArgs} = require("../parseConfigFromArgs");
const DDLManager = require("../../dist/DDLManager");

module.exports = async function(argv) {
    let config = parseConfigFromArgs(argv);

    await DDLManager.refreshCache({
        db: config,
        folder: config.folder
    });
};