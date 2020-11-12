"use strict";

const DDLManager = require("../../lib/DDLManager");
const dbConfig = require("../ddl-manager-config");

(async function() {
    try {
        
        await DDLManager.watch({
            folder: __dirname + "/ddl",
            // user, password, database, port, host
            db: dbConfig
        });

    } catch(err) {
        console.error(err);
        process.exit(1);
    }
})();
