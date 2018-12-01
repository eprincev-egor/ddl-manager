"use strict";

const DdlManager = require("../../lib/DdlManager");
const dbConfig = require("../database-config");
const fs = require("fs");

(async function() {
    try {
        const dir = __dirname + "/ddl";

        if ( !fs.existsSync(dir)  ) {
            fs.mkdirSync(dir);
        }
        
        await DdlManager.dump({
            folder: dir,
            // user, password, database, port, host
            db: dbConfig
        });

    } catch(err) {
        console.error(err);
        process.exit(1);
    }
})();
