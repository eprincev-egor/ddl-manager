import fs from "fs";
import path from "path";
import assert from "assert";
import { CacheTriggersBuilder } from "../../../../lib/cache/CacheTriggersBuilder";
import { testDatabase } from "./testDatabase";

export interface ITest {
    testDir: string;
    tables: string[];
}

export function testTriggers(test: ITest) {
    
    const cacheFilePath = path.join(test.testDir, "cache.sql");
    const cacheSQL = fs.readFileSync(cacheFilePath).toString();


    const builder = new CacheTriggersBuilder(
        cacheSQL,
        testDatabase
    );
    const triggers = builder.createTriggers();

    for (let schemaTable of test.tables) {
        const triggerFilePath = path.join(test.testDir, schemaTable + ".sql");
        const expectedTriggerDDL = fs.readFileSync(triggerFilePath).toString();

        schemaTable = schemaTable.split(".").slice(0, 2).join(".");

        const output = triggers.find(trigger => 
            expectedTriggerDDL.includes(trigger.name)
        );
        assert.ok(output, "should be trigger for table: " + schemaTable);

        const actualTriggerDDL = (
            output.function.toSQL() + 
            ";\n\n" + 
            output.trigger.toSQL() +
            ";"
        );

        assert.strictEqual(
            actualTriggerDDL,
            expectedTriggerDDL
        )
    }
}
