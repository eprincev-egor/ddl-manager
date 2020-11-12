import fs from "fs";
import path from "path";
import assert from "assert";
import { CacheTriggerFactory } from "../../../../lib/cache/CacheTriggerFactory";

export interface ITest {
    testDir: string;
    tables: string[];
}

export function testTriggers(test: ITest) {
    
    const cacheFilePath = path.join(test.testDir, "cache.sql");
    const cacheSQL = fs.readFileSync(cacheFilePath).toString();

    const fabric = new CacheTriggerFactory();
    const triggers = fabric.createTriggers(cacheSQL);

    for (const schemaTable of test.tables) {

        const triggerFilePath = path.join(test.testDir, schemaTable + ".sql");
        const expectedTriggerDDL = fs.readFileSync(triggerFilePath).toString();

        const output = triggers[schemaTable];
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
