import fs from "fs";
import path from "path";
import assert from "assert";
import { CacheTriggersBuilder } from "../../../../lib/cache/CacheTriggersBuilder";
import { testDatabase } from "./testDatabase";
import { CacheParser } from "../../../../lib/parser";

export interface ITest {
    testDir: string;
    tables: string[];
}

export function testTriggers(test: ITest) {
    
    const cacheFilePath = path.join(test.testDir, "cache.sql");
    const cacheSQL = fs.readFileSync(cacheFilePath).toString();
    const cache = CacheParser.parse(cacheSQL);


    const builder = new CacheTriggersBuilder(
        [cache], cache,
        testDatabase
    );
    const triggers = builder.createTriggers();

    for (let fileName of test.tables) {
        const triggerFilePath = path.join(test.testDir, fileName + ".sql");
        const expectedTriggerDDL = fs.readFileSync(triggerFilePath).toString();

        const triggerName = fileName.replace(".sql", "");

        const output = triggers.find(trigger => 
            expectedTriggerDDL.includes(trigger.name)
        );
        assert.ok(output, "should be trigger: " + triggerName);

        const actualTriggerDDL = (
            output.function.toSQL() + 
            ";\n\n" + 
            output.trigger.toSQL() +
            ";"
        );

        // fs.writeFileSync(triggerFilePath, actualTriggerDDL);
        assert.strictEqual(
            actualTriggerDDL,
            expectedTriggerDDL
        )
    }
    // TODO: throw error on missed test for trigger
}
