import { Comparator } from "../../../lib/Comparator/Comparator";
import { Database } from "../../../lib/database/schema/Database";
import { FilesState } from "../../../lib/fs/FilesState";
import { deepStrictEqualMigration } from "./deepStrictEqualMigration";
import assert from "assert";
import {
    testFileWithCache
} from "./fixture/cache-fixture";

describe("Comparator: compare cache", () => {
    
    let database!: Database;
    let fs!: FilesState;
    beforeEach(() => {
        database = new Database();
        fs = new FilesState();
    });

    it("sync empty state", () => {
        const migration = Comparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                columns: [],
                updates: []
            },
            create: {
                updates: []
            }
        });
    });

    it("create simple cache", () => {
        
        fs.addFile(testFileWithCache);

        const migration = Comparator.compare(database, fs);

        assert.strictEqual(migration.toDrop.columns.length, 0, "no columns to drop");
        assert.strictEqual(migration.toCreate.columns.length, 1, "one column to create");
        
        const actualColumn = migration.toCreate.columns[0];
        assert.deepStrictEqual(actualColumn.toJSON(), {
            table: {
                schema: "public",
                name: "companies"
            },
            name: "orders_profit",
            type: "numeric",
            "default": "0",
            cacheSignature: "cache totals for companies",
            comment: actualColumn.comment
        });

        assert.strictEqual(migration.toCreate.updates.length, 1, "one update for columns");
        assert.strictEqual(
            migration.toCreate.updates[0].forTable.toString(),
            "companies"
        );
        assert.strictEqual(
            migration.toCreate.updates[0].select.columns[0].toString().trim(),
            "coalesce(sum(orders.profit), 0) as orders_profit"
        );
    });

});