"use strict";

const DDLManager = require("../../lib/DDLManager");
const dbConfig = require("../ddl-manager-config");

(async function() {
    try {
        
        await DDLManager.build({
            folder: __dirname + "/ddl",
            // user, password, database, port, host
            db: dbConfig
        });

        process.exit(0);
    } catch(err) {
        console.error(err);
        process.exit(1);
    }
})();
