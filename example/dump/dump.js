"use strict";

const DdlManager = require("../../lib/DdlManager");
const dbConfig = require("../database-config");

(async function() {
    try {
        
        await DdlManager.dump({
            folder: __dirname + "/ddl",
            // user, password, database, port, host
            db: dbConfig
        });

    } catch(err) {
        console.error(err);
        process.exit(1);
    }
})();
