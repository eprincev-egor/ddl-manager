"use strict";

const DDLManager = require("../../lib/DDLManager");
const dbConfig = require("../ddl-manager-config");
const fs = require("fs");

(async function() {
    try {
        const dir = __dirname + "/ddl";

        if ( !fs.existsSync(dir)  ) {
            fs.mkdirSync(dir);
        }
        
        await DDLManager.dump({
            folder: dir,
            // user, password, database, port, host
            db: dbConfig,

            // if set true, then build will without frozen errors 
            unfreeze: false
        });

    } catch(err) {
        console.error(err);
        process.exit(1);
    }
})();
