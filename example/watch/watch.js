"use strict";

const DdlManager = require("../../lib/DdlManager");
const dbConfig = require("../ddl-manager-config");

(async function() {
    try {
        
        await DdlManager.watch({
            folder: __dirname + "/ddl",
            // user, password, database, port, host
            db: dbConfig
        });

    } catch(err) {
        console.error(err);
        process.exit(1);
    }
})();
