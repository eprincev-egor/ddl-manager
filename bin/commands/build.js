"use strict";

const {parseConfigFromArgs} = require("../../lib/utils");
const DdlManager = require("../../lib/DdlManager");

module.exports = async function(argv) {
    let config = parseConfigFromArgs(argv);

    await DdlManager.build({
        db: config,
        folder: config.folder
    });
};