import fs from "fs";
import fse from "fs-extra";
import { DDLManager } from "../lib/DDLManager";
import { getDBClient } from "./integration/getDbClient";

getDBClient()
    .then(async(db) => {
        const folder = __dirname + "/sandbox";
        await DDLManager.dump({
            db,
            folder
        });
        fse.removeSync(folder);
        fs.mkdirSync(folder);

        process.exit(0);
    })
    .catch(err => {
        console.error(err);
        process.exit(1);
    })
;