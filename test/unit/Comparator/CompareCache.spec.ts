import { MainComparator } from "../../../lib/Comparator/MainComparator";
import { Database } from "../../../lib/database/schema/Database";
import { FilesState } from "../../../lib/fs/FilesState";
import { deepStrictEqualMigration } from "./deepStrictEqualMigration";
import assert from "assert";
import {
    someFileParams,
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
import { FakeDatabaseDriver } from "../FakeDatabaseDriver";
import { FileParser } from "../../../lib/parser";

describe("Comparator: compare cache", async() => {
    
    let postgres!: FakeDatabaseDriver;
    let database!: Database;
    let fs!: FilesState;
    beforeEach(() => {
        database = new Database();
        fs = new FilesState();
        postgres = new FakeDatabaseDriver();
    });

    it("sync empty state", async() => {
        const migration = await MainComparator.compare(postgres, database, fs);

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

    it("create simple cache", async() => {
        
        fs.addFile(testFileWithCache);

        const {toCreate, toDrop} = await MainComparator.compare(postgres, database, fs);

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

    it("drop cache", async() => {
        
        database.addFunctions([ testCacheFunc ]);
        database.setTable(testTableWithCache);
        database.setTable(testTableSource);
        database.addTrigger(testCacheTrigger);

        const {toDrop, toCreate} = await MainComparator.compare(postgres, database, fs);

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

    it("no changes => empty migration", async() => {

        database.addFunctions([ testCacheFunc ]);
        database.setTable(testTableWithCache);
        database.setTable(testTableSource);
        database.addTrigger(testCacheTrigger);

        fs.addFile(testFileWithCache);

        const {toDrop, toCreate} = await MainComparator.compare(postgres, database, fs);

        assert.strictEqual(toCreate.updates.length, 0, "no updates");
        assert.strictEqual(toCreate.columns.length, 0, "no columns to create");
        assert.strictEqual(toCreate.functions.length, 0, "no funcs to create");
        assert.strictEqual(toCreate.triggers.length, 0, "no triggers to create");

        assert.strictEqual(toDrop.columns.length, 0, "no columns to drop");
        assert.strictEqual(toDrop.functions.length, 0, "no funcs to drop");
        assert.strictEqual(toDrop.triggers.length, 0, "no triggers to drop");
    });

    it("don't update column if was just cache renaming, but recreate triggers and funcs", async() => {

        database.addFunctions([ testCacheFunc ]);
        database.setTable(testTableWithCache);
        database.setTable(testTableSource);
        database.addTrigger(testCacheTrigger);

        fs.addFile(testFileWithCacheWithOtherName);

        const {toDrop, toCreate} = await MainComparator.compare(postgres, database, fs);

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

    it("drop column if changed type", async() => {

        database.addFunctions([ testCacheFunc ]);
        database.setTable(testTableWithCache);
        database.setTable(testTableSource);
        database.addTrigger(testCacheTrigger);

        fs.addFile(testFileWithCacheWithOtherColumnType);

        const {toDrop, toCreate} = await MainComparator.compare(postgres, database, fs);

        assert.strictEqual(toCreate.columns.length, 1, "one column to recreate comment");
        assert.strictEqual(toCreate.updates.length, 1, "one update");
        assert.strictEqual(toCreate.functions.length, 1, "one func to create");
        assert.strictEqual(toCreate.triggers.length, 1, "one trigger to create");
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

        assert.strictEqual(toDrop.columns.length, 1, "one column to drop");
        assert.strictEqual(toDrop.functions.length, 1, "one func to drop");
        assert.strictEqual(toDrop.triggers.length, 1, "one trigger to drop");

    });

    it("update column if changed select", async() => {

        database.addFunctions([ testCacheFunc ]);
        database.setTable(testTableWithCache);
        database.setTable(testTableSource);
        database.addTrigger(testCacheTrigger);

        fs.addFile(testFileWithCacheWithOtherCalc);

        const {toDrop, toCreate} = await MainComparator.compare(postgres, database, fs);

        assert.strictEqual(toCreate.columns.length, 1, "one column to recreate comment");
        assert.strictEqual(toCreate.updates.length, 1, "one update");
        assert.strictEqual(toCreate.functions.length, 1, "one func to create");
        assert.strictEqual(toCreate.triggers.length, 1, "one trigger to create");
        
        assert.strictEqual(toDrop.columns.length, 0, "no columns to drop");
        assert.strictEqual(toDrop.functions.length, 1, "one func to drop");
        assert.strictEqual(toDrop.triggers.length, 1, "one trigger to drop");
    });

    it("create cache columns with hard expression", async() => {

        const hardCache = FileParser.parseCache(`
            cache totals for companies (
                select

                    array_agg( '2012-10-10'::date ) as some_dates,
                    max( orders.dt_create::text ) as max_text,
                    (sum( orders.id ) * 2)::text as sum_text

                from orders
                where
                    orders.id_client = companies.id
            )
        `);

        const testFileWithHardCache = {
            ...someFileParams,
            content: {
                cache: [hardCache]
            }
        };
        fs.addFile(testFileWithHardCache);

        postgres.setColumnsTypes({
            some_dates: "date[]",
            max_text_dt_create: "text[]",
            max_text: "text",
            sum_text: "text"
        });

        const {toCreate} = await MainComparator.compare(postgres, database, fs);

        const actualColumns = toCreate.columns.map(column => column.toJSON());
        assert.deepStrictEqual(actualColumns, [
            {
                table: {
                    schema: "public",
                    name: "companies"
                },
                name: "some_dates",
                type: "date[]",
                default: "null",
                cacheSignature: "cache totals for companies",
                comment: actualColumns[0].comment
            },
            {
                table: {
                    schema: "public",
                    name: "companies"
                },
                name: "max_text_dt_create",
                type: "text[]",
                default: "null",
                cacheSignature: "cache totals for companies",
                comment: actualColumns[1].comment
            },
            {
                table: {
                    schema: "public",
                    name: "companies"
                },
                name: "max_text",
                type: "text",
                default: "null",
                cacheSignature: "cache totals for companies",
                comment: actualColumns[2].comment
            },
            {
                table: {
                    schema: "public",
                    name: "companies"
                },
                name: "sum_text_sum",
                type: "numeric",
                default: "0",
                cacheSignature: "cache totals for companies",
                comment: actualColumns[3].comment
            },
            {
                table: {
                    schema: "public",
                    name: "companies"
                },
                name: "sum_text",
                type: "text",
                default: "0",
                cacheSignature: "cache totals for companies",
                comment: actualColumns[4].comment
            }
        ]);

        assert.strictEqual(toCreate.updates.length, 1, "refresh cache per one update");
        assert.deepStrictEqual(
            toCreate.updates[0].select.columns.map(column => column.name),
            ["some_dates", "max_text_dt_create", "max_text", "sum_text_sum", "sum_text"],
            "refresh cache per one update"
        );

    });

});