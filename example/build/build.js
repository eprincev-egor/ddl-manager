"use strict";

const DdlManager = require("../../DdlManager");
const dbConfig = require("../database-config");

(async function() {
    try {
        
        await DdlManager.build({
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
