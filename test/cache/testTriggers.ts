import fs from "fs";
import path from "path";
import assert from "assert";
import { TriggerFactory } from "../../lib/cache/TriggerFactory";

export interface ITest {
    testDir: string;
    tables: string[];
}

export function testTriggers(test: ITest) {
    
    const cacheFilePath = path.join(test.testDir, "cache.sql");
    const cacheSQL = fs.readFileSync(cacheFilePath).toString();

    const fabric = new TriggerFactory();
    const triggers = fabric.createTriggers(cacheSQL);

    for (const schemaTable of test.tables) {

        const triggerFilePath = path.join(test.testDir, schemaTable + ".sql");
        const expectedTriggerDDL = fs.readFileSync(triggerFilePath).toString();

        const trigger = triggers[schemaTable];
        const actualTriggerDDL = trigger.toString();

        assert.strictEqual(
            actualTriggerDDL,
            expectedTriggerDDL
        )
    }
}
