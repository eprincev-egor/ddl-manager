import fs from "fs";
import path from "path";
import assert from "assert";
import { CacheTriggersBuilder } from "../../../../lib/cache/CacheTriggersBuilder";
import { Database } from "../../../../lib/database/schema/Database";
import { Table } from "../../../../lib/database/schema/Table";
import { Column } from "../../../../lib/database/schema/Column";
import { TableID } from "../../../../lib/database/schema/TableID";

export interface ITest {
    testDir: string;
    tables: string[];
}

export function testTriggers(test: ITest) {
    
    const cacheFilePath = path.join(test.testDir, "cache.sql");
    const cacheSQL = fs.readFileSync(cacheFilePath).toString();

    const testDatabase = new Database([
        new Table(
            "public",
            "orders",
            [
                new Column(
                    new TableID(
                        "public",
                        "orders",
                    ),
                    "companies_ids",
                    "integer[]"
                ),
                new Column(
                    new TableID(
                        "public",
                        "orders",
                    ),
                    "clients_ids",
                    "integer[]"
                )
            ]
        ),
        new Table(
            "public",
            "vats",
            [
                new Column(
                    new TableID(
                        "public",
                        "vats",
                    ),
                    "vat_value",
                    "numeric"
                )
            ]
        )
    ]);

    const builder = new CacheTriggersBuilder(
        cacheSQL,
        testDatabase
    );
    const triggers = builder.createTriggers();

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
