import { MainComparator } from "../../../lib/Comparator/MainComparator";
import { Database } from "../../../lib/database/schema/Database";
import { FilesState } from "../../../lib/fs/FilesState";
import { deepStrictEqualMigration } from "./deepStrictEqualMigration";
import assert from "assert";
import {
    testFileWithCache,
    testCacheFunc,
    testCacheTrigger,
    testTableWithCache,
    testTableSource,
    testCacheColumn,
    testCacheWithOtherName,
    testFileWithCacheWithOtherName,
    testFileWithCacheWithOtherCalc,
    testCacheWithOtherColumnType,
    testFileWithCacheWithOtherColumnType
} from "./fixture/cache-fixture";

describe("Comparator: compare cache", () => {
    
    let database!: Database;
    let fs!: FilesState;
    beforeEach(() => {
        database = new Database();
        fs = new FilesState();
    });

    it("sync empty state", () => {
        const migration = MainComparator.compare(database, fs);

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

        const {toCreate, toDrop} = MainComparator.compare(database, fs);

        assert.strictEqual(toDrop.columns.length, 0, "no columns to drop");
        assert.strictEqual(toCreate.columns.length, 1, "one column to create");
        
        const actualColumn = toCreate.columns[0];
        assert.deepStrictEqual(actualColumn.toJSON(), {
            table: {
                schema: "public",
                name: "companies"
            },
            name: "orders_profit",
            type: "numeric",
            "default": "0",
            cacheSignature: "cache totals for companies",
            comment: actualColumn.comment.toString()
        });

        assert.strictEqual(toCreate.updates.length, 1, "one update for columns");
        assert.strictEqual(
            toCreate.updates[0].forTable.toString(),
            "companies"
        );
        assert.strictEqual(
            toCreate.updates[0].select.columns[0].toString().trim(),
            "coalesce(sum(orders.profit), 0) as orders_profit"
        );

        assert.strictEqual(toCreate.triggers.length, 1, "one cache func to create");
        assert.strictEqual(toCreate.functions.length, 1, "one cache trigger to create");

        assert.strictEqual(toCreate.functions[0].cacheSignature, "cache totals for companies");
        assert.strictEqual(toCreate.triggers[0].cacheSignature, "cache totals for companies");

        assert.deepStrictEqual(
            toCreate.triggers[0].table.toString(),
            "public.orders"
        );
    });

    it("drop cache", () => {
        
        database.addFunctions([ testCacheFunc ]);
        database.setTable(testTableWithCache);
        database.setTable(testTableSource);
        database.addTrigger(testCacheTrigger);

        const {toDrop, toCreate} = MainComparator.compare(database, fs);

        assert.strictEqual(toCreate.columns.length, 0, "no columns to create");
        assert.strictEqual(toCreate.updates.length, 0, "no updates to create");
        assert.strictEqual(toCreate.triggers.length, 0, "no triggers to create");
        assert.strictEqual(toCreate.functions.length, 0, "no funcs to create");

        assert.strictEqual(toDrop.triggers.length, 1, "one trigger to drop");
        assert.strictEqual(toDrop.functions.length, 1, "one func to drop");
        assert.strictEqual(toDrop.columns.length, 1, "one column to drop");

        assert.deepStrictEqual(toDrop.columns[0].toJSON(), {
            table: {
                schema: "public",
                name: "companies"
            },
            name: "orders_profit",
            type: "numeric",
            "default": "0",
            cacheSignature: "cache totals for companies",
            comment: testCacheColumn.comment.toString()
        });
    });

    it("no changes => empty migration", () => {

        database.addFunctions([ testCacheFunc ]);
        database.setTable(testTableWithCache);
        database.setTable(testTableSource);
        database.addTrigger(testCacheTrigger);

        fs.addFile(testFileWithCache);

        const {toDrop, toCreate} = MainComparator.compare(database, fs);

        assert.strictEqual(toCreate.updates.length, 0, "no updates");
        assert.strictEqual(toCreate.columns.length, 0, "no columns to create");
        assert.strictEqual(toCreate.triggers.length, 0, "not triggers to create");
        // cache fixture has wrong function body
        // assert.strictEqual(toCreate.functions.length, 0, "no funcs to create");

        assert.strictEqual(toDrop.columns.length, 0, "no columns to drop");
        assert.strictEqual(toDrop.triggers.length, 0, "no triggers to drop");
        // cache fixture has wrong function body
        // assert.strictEqual(toDrop.functions.length, 0, "no funcs to drop");
    });

    it("don't update column if was just cache renaming, but recreate triggers and funcs", () => {

        database.addFunctions([ testCacheFunc ]);
        database.setTable(testTableWithCache);
        database.setTable(testTableSource);
        database.addTrigger(testCacheTrigger);

        fs.addFile(testFileWithCacheWithOtherName);

        const {toDrop, toCreate} = MainComparator.compare(database, fs);

        assert.strictEqual(toCreate.updates.length, 0, "no updates to create");
        assert.strictEqual(toCreate.triggers.length, 1, "one trigger to create");
        assert.strictEqual(toCreate.functions.length, 1, "one func to create");
        assert.strictEqual(toCreate.columns.length, 1, "one column to recreate comment");
        assert.deepStrictEqual(toCreate.columns[0].toJSON(), {
            table: {
                schema: "public",
                name: "companies"
            },
            name: "orders_profit",
            type: "numeric",
            "default": "0",
            cacheSignature: testCacheWithOtherName.getSignature(),

            // no matter...
            comment: toCreate.columns[0].comment.toString()
        });

        assert.strictEqual(toDrop.triggers.length, 1, "one trigger to drop");
        assert.strictEqual(toDrop.functions.length, 1, "one func to drop");
        assert.strictEqual(toDrop.columns.length, 0, "no columns to drop");

    });

    it("drop column if changed type", () => {

        database.addFunctions([ testCacheFunc ]);
        database.setTable(testTableWithCache);
        database.setTable(testTableSource);
        database.addTrigger(testCacheTrigger);

        fs.addFile(testFileWithCacheWithOtherColumnType);

        const {toDrop, toCreate} = MainComparator.compare(database, fs);

        assert.strictEqual(toCreate.updates.length, 1, "one update");
        assert.strictEqual(toCreate.triggers.length, 0, "no triggers to create");
        assert.strictEqual(toCreate.functions.length, 1, "one func to create");
        assert.strictEqual(toCreate.columns.length, 1, "one column to recreate comment");
        assert.deepStrictEqual(toCreate.columns[0].toJSON(), {
            table: {
                schema: "public",
                name: "companies"
            },
            name: "orders_profit",
            type: "integer[]",
            "default": "null",
            cacheSignature: testCacheWithOtherColumnType.getSignature(),

            // no matter...
            comment: toCreate.columns[0].comment.toString()
        });

        assert.strictEqual(toDrop.triggers.length, 0, "no triggers to drop");
        assert.strictEqual(toDrop.functions.length, 1, "one func to drop");
        assert.strictEqual(toDrop.columns.length, 1, "one column to drop");

    });

    it("update column if changed select", () => {

        database.addFunctions([ testCacheFunc ]);
        database.setTable(testTableWithCache);
        database.setTable(testTableSource);
        database.addTrigger(testCacheTrigger);

        fs.addFile(testFileWithCacheWithOtherCalc);

        const {toDrop, toCreate} = MainComparator.compare(database, fs);

        assert.strictEqual(toCreate.updates.length, 1, "one update");
        assert.strictEqual(toCreate.triggers.length, 0, "no triggers to create");
        assert.strictEqual(toCreate.functions.length, 1, "one func to create");
        assert.strictEqual(toCreate.columns.length, 1, "one column to recreate comment");
        
        assert.strictEqual(toDrop.triggers.length, 0, "no triggers to drop");
        assert.strictEqual(toDrop.functions.length, 1, "one func to drop");
        assert.strictEqual(toDrop.columns.length, 0, "no columns to drop");
    });

});