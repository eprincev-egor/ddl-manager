import { testDatabase } from "./testDatabase";
import { FakeDatabaseDriver } from "../../FakeDatabaseDriver";
import { FileParser } from "../../../../lib/parser";
import { FilesState } from "../../../../lib/fs/FilesState";
import { MainComparator } from "../../../../lib/Comparator/MainComparator";
import fs from "fs";
import path from "path";
import assert from "assert";

export interface ITest {
    testDir: string;
    tables: string[];
}

export async function testTriggers(test: ITest) {
    
    const cacheFilePath = path.join(test.testDir, "cache.sql");
    const cacheFileContent = FileParser.parseFile(cacheFilePath);

    const fsState = new FilesState();
    fsState.addFile({
        name: "cache.sql",
        folder: test.testDir,
        path: cacheFilePath,
        content: cacheFileContent
    });
    
    const migration = await MainComparator.compare(
        new FakeDatabaseDriver(),
        testDatabase,
        fsState
    );

    for (let fileName of test.tables) {
        const triggerFilePath = path.join(test.testDir, fileName + ".sql");
        const expectedTriggerDDL = fs.readFileSync(triggerFilePath).toString();

        const triggerName = fileName.replace(".sql", "");

        const actualTrigger = migration.toCreate.triggers.find(trigger => 
            expectedTriggerDDL.includes(trigger.name)
        );
        const actualProcedure = migration.toCreate.functions.find(trigger => 
            expectedTriggerDDL.includes(trigger.name)
        );
        assert.ok(
            actualTrigger && actualProcedure, 
            "should be trigger: " + triggerName
        );

        const actualTriggerDDL = (
            actualProcedure.toSQL() + 
            ";\n\n" + 
            actualTrigger.toSQL() +
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
