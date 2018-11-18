"use strict";

const fs = require("fs");

fs.writeFileSync("./database-config.js", `
"use strict";

module.exports = {
    host: "localhost",
    user: "ubuntu",
    password: "ubuntu",
    port: 5432,
    database: "ddl-manager"
};
`);
