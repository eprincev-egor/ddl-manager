import fs from "fs";
import glob from "glob";
import { testTriggers } from "./testTriggers";

describe("TriggerFabric", () => {

    const cacheFiles = glob.sync(__dirname + "/**/cache.sql");

    for (const cacheFilePath of cacheFiles) {
        const {
            dirPath: testDir, 
            dirName: testName
        } = parsePath(cacheFilePath);

        if ( /^x-/.test(testName) ) {
            continue;
        }
        // if ( testName !== "string_agg_where_by_arr_and_joins" ) {
        //     continue;
        // }

        const files = fs.readdirSync(testDir);

        const tables = files
            .filter(fileName =>
                /\.sql$/.test(fileName) &&
                fileName !== "cache.sql"
            )
            .map(fileName =>
                fileName.replace(".sql", "")
            );

        it(testDir.replace(__dirname, ""), () => {
            testTriggers({
                testDir,
                tables
            });
        });
    }

    // "/a/b/c/x.sql"  => {dirName: "c", dirPath: "/a/b/c"}
    function parsePath(filePath: string) {
        const dirNamesAndFileName = filePath.split("/");

        const dirPath = dirNamesAndFileName.slice(0, -1).join("/");
        const dirName = dirNamesAndFileName.slice(-2)[0];

        return {dirPath, dirName};
    }
});