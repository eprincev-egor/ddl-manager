import assert from "assert";
import { FakeDatabase } from "./FakeDatabase";
import { MainMigrator } from "../../../lib/Migrator/MainMigrator";
import { Migration } from "../../../lib/Migrator/Migration";
import { DatabaseFunction } from "../../../lib/database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../../../lib/database/schema/DatabaseTrigger";
import { TableID } from "../../../lib/database/schema/TableID";
import { FileParser } from "../../../lib/parser";


describe("Migrator", () => {

    let database!: FakeDatabase;
    let changes!: Migration;
    beforeEach(() => {
        database = new FakeDatabase();
        changes = Migration.empty();
    });

    const ordersProfitCacheSQL = `
        cache test for some_table (
            select
                sum( another_table.profit ) as orders_profit
            from another_table
            where
                another_table.id_client = some_table.id
        )
    `;

    it("create function", async() => {
        changes.create({
            functions: [
                new DatabaseFunction({
                    schema: "public",
                    name: "some_simple_func",
                    args: [],
                    returns: {type: "void"},
                    body: ""
                })
            ]
        });

        await MainMigrator.migrate(database, changes);

        assert.strictEqual(
            database.state.functions.length,
            1
        );
        assert.strictEqual(
            database.state.functions[0].getSignature(),
            "public.some_simple_func()"
        );
    });

    it("create trigger", async() => {

        changes.create({
            triggers: [
                new DatabaseTrigger({
                    name: "some_simple_trigger",
                    table: new TableID(
                        "public",
                        "some_table"
                    ),
                    procedure: {
                        schema: "public",
                        name: "some_trigger_func",
                        args: []
                    }
                })
            ]
        });

        await MainMigrator.migrate(database, changes);

        assert.strictEqual(
            database.state.triggers.length,
            1
        );
        assert.strictEqual(
            database.state.triggers[0].getSignature(),
            "some_simple_trigger on public.some_table"
        );
    });

    it("create cache triggers and columns", async() => {

        database.setColumnsTypes({
            orders_profit: "numeric"
        });

        changes.create({
            // cache: [FileParser.parseCache(ordersProfitCacheSQL)]
        });

        await MainMigrator.migrate(database, changes);

        assert.deepStrictEqual(database.columns, {
            "public.some_table.orders_profit": {
                name: "orders_profit",
                type: "numeric",
                "default": "0"
            }
        });

        assert.strictEqual(
            database.state.triggers.length,
            1
        );
        assert.strictEqual(
            database.state.triggers[0].getSignature(),
            "cache_test_for_some_table_on_another_table on public.another_table"
        );

        assert.strictEqual(
            database.state.functions.length,
            1
        );
        assert.strictEqual(
            database.state.functions[0].getSignature(),
            "public.cache_test_for_some_table_on_another_table()"
        );
    });

    it("update cache by packages", async() => {

        database.setColumnsTypes({
            orders_profit: "numeric"
        });

        const cache = FileParser.parseCache(ordersProfitCacheSQL);
        changes.create({
            // cache: [cache]
        });

        database.setRowsCount(cache.for.table.toString(), 1499);

        await MainMigrator.migrate(database, changes);

        assert.deepStrictEqual(
            database.getUpdatedPackages(cache.for.table.toString()),
            [{limit: 500}, {limit: 500}, {limit: 500}]
        );
    });


    it("create cache helpers columns for string_agg", async() => {

        database.setColumnsTypes({
            doc_numbers_array_agg: "text[]",
            doc_numbers: "text"
        });

        changes.create({
            // cache: [FileParser.parseCache(`
            //     cache test for some_table (
            //         select
            //             string_agg( another_table.doc_number, ', ' ) as doc_numbers
            //         from another_table
            //         where
            //             another_table.id_client = some_table.id
            //     )
            // `)]
        });

        await MainMigrator.migrate(database, changes);

        assert.deepStrictEqual(database.columns, {
            "public.some_table.doc_numbers_array_agg": {
                name: "doc_numbers_array_agg",
                type: "text[]",
                "default": "null"
            },
            "public.some_table.doc_numbers": {
                name: "doc_numbers",
                type: "text",
                "default": "null"
            }
        });
    });


    it("create cache helpers columns for hard expression", async() => {

        database.setColumnsTypes({
            some_profit_sum_profit: "numeric",
            some_profit_sum_xxx: "numeric",
            some_profit: "numeric"
        });

        changes.create({
            // cache: [FileParser.parseCache(`
            //     cache test for some_table (
            //         select
            //             sum( another_table.profit ) * 2 + 
            //             sum( another_table.xxx )
            //             as some_profit
            //         from another_table
            //         where
            //             another_table.id_client = some_table.id
            //     )
            // `)]
        });

        await MainMigrator.migrate(database, changes);

        assert.deepStrictEqual(database.columns, {
            "public.some_table.some_profit_sum_profit": {
                name: "some_profit_sum_profit",
                type: "numeric",
                "default": "0"
            },
            "public.some_table.some_profit_sum_xxx": {
                name: "some_profit_sum_xxx",
                type: "numeric",
                "default": "0"
            },
            "public.some_table.some_profit": {
                name: "some_profit",
                type: "numeric",
                "default": "0"
            }
        });
    });

    it("drop cache triggers and columns", async() => {
        
        const testCache = FileParser.parseCache(ordersProfitCacheSQL);

        database.setColumnsTypes({
            orders_profit: "numeric"
        });

        changes.create({
            // cache: [testCache]
        });

        await MainMigrator.migrate(database, changes);

        assert.deepStrictEqual(database.columns, {
            "public.some_table.orders_profit": {
                name: "orders_profit",
                type: "numeric",
                "default": "0"
            }
        });

        assert.strictEqual(database.state.triggers.length, 1);


        const dropChanges = Migration.empty().drop({
            // cache: [testCache]
        });
        await MainMigrator.migrate(database, dropChanges);


        assert.deepStrictEqual(database.columns, {
        });

        assert.strictEqual(database.state.triggers.length, 0);
    });

    it("don't drop cache columns, but recreate triggers if was just cache renaming", async() => {

        const cacheBeforeRenamingSQL = `
            cache before_rename for some_table (
                select
                    sum( my_table.profit ) as orders_profit
                from my_table
                where
                    my_table.id_client = some_table.id
            )
        `;
        const cacheBeforeRenaming = FileParser.parseCache(cacheBeforeRenamingSQL);

        const cacheAfterRenamingSQL = `
            cache after_rename for some_table (
                select
                    sum( my_table.profit ) as orders_profit
                from my_table
                where
                    my_table.id_client = some_table.id
            )
        `;
        const cacheAfterRenaming = FileParser.parseCache(cacheAfterRenamingSQL);


        database.setColumnsTypes({
            orders_profit: "numeric"
        });
        database.setRowsCount("public.some_table", 1400);

        await MainMigrator.migrate(database, Migration.empty().create({
            // cache: [cacheBeforeRenaming]
        }));
        assert.deepStrictEqual(database.columns, {
            "public.some_table.orders_profit": {
                name: "orders_profit",
                type: "numeric",
                "default": "0"
            }
        });
        assert.strictEqual(
            database.state.triggers[0].name,
            "cache_before_rename_for_some_table_on_my_table"
        );


        await MainMigrator.migrate(database, Migration.empty()
            .drop({
                // cache: [cacheBeforeRenaming]
            })
            .create({
                // cache: [cacheAfterRenaming]
            })
        );
        assert.deepStrictEqual(database.columns, {
            "public.some_table.orders_profit": {
                name: "orders_profit",
                type: "numeric",
                "default": "0"
            }
        });
        assert.strictEqual(
            database.state.triggers[0].name,
            "cache_after_rename_for_some_table_on_my_table"
        );

        assert.strictEqual(
            database.wasDroppedColumn("public.some_table", "orders_profit"),
            false,
            "check column drop"
        );
        assert.deepStrictEqual(
            database.getUpdatedPackages("public.some_table"),
            [{limit: 500}, {limit: 500}, {limit: 500}]
        );
    });

    it("return error on creating cache", async() => {

        database.setColumnsTypes({
            orders_profit: "numeric"
        });

        database.createOrReplaceCacheTrigger = async () => {
            throw new Error("test error");
        };

        changes.create({
            // cache: [FileParser.parseCache(ordersProfitCacheSQL)]
        });

        const errors = await MainMigrator.migrate(database, changes);

        assert.strictEqual(errors.length, 1);
    });

});
