"use strict";

const {parseConfigFromArgs} = require("../parseConfigFromArgs");
const DDLManager = require("../../dist/DDLManager");

module.exports = async function(argv) {
    const config = parseConfigFromArgs(argv);

    const migration = await DDLManager.compare({
        db: config,
        folder: config.folder
    });
    migration.log();
};