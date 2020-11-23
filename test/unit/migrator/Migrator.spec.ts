import assert from "assert";
import { FakeDatabase } from "./FakeDatabase";
import { Migrator } from "../../../lib/Migrator";
import { Diff } from "../../../lib/Diff";
import { Cache, DatabaseFunction, DatabaseTrigger } from "../../../lib/ast";
import { FileParser } from "../../../lib/parser";


describe("Migrator", () => {

    let database!: FakeDatabase;
    let changes!: Diff;
    beforeEach(() => {
        database = new FakeDatabase();
        changes = Diff.empty();
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
        changes.createState({
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

        await Migrator.migrate(database, changes);

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

        changes.createState({
            triggers: [
                new DatabaseTrigger({
                    name: "some_simple_trigger",
                    table: {
                        schema: "public",
                        name: "some_table"
                    },
                    procedure: {
                        schema: "public",
                        name: "some_trigger_func",
                        args: []
                    }
                })
            ]
        });

        await Migrator.migrate(database, changes);

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

        changes.createState({cache: [FileParser.parseCache(ordersProfitCacheSQL)]});

        await Migrator.migrate(database, changes);

        assert.deepStrictEqual(database.columns, {
            "public.some_table.orders_profit": {
                key: "orders_profit",
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
        changes.createState({cache: [cache]});

        database.setRowsCount(cache.for.table.toString(), 1499);

        await Migrator.migrate(database, changes);

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

        changes.createState({cache: [FileParser.parseCache(`
            cache test for some_table (
                select
                    string_agg( another_table.doc_number, ', ' ) as doc_numbers
                from another_table
                where
                    another_table.id_client = some_table.id
            )
        `)]});

        await Migrator.migrate(database, changes);

        assert.deepStrictEqual(database.columns, {
            "public.some_table.doc_numbers_array_agg": {
                key: "doc_numbers_array_agg",
                type: "text[]",
                "default": "null"
            },
            "public.some_table.doc_numbers": {
                key: "doc_numbers",
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

        changes.createState({cache: [FileParser.parseCache(`
            cache test for some_table (
                select
                    sum( another_table.profit ) * 2 + 
                    sum( another_table.xxx )
                    as some_profit
                from another_table
                where
                    another_table.id_client = some_table.id
            )
        `)]});

        await Migrator.migrate(database, changes);

        assert.deepStrictEqual(database.columns, {
            "public.some_table.some_profit_sum_profit": {
                key: "some_profit_sum_profit",
                type: "numeric",
                "default": "0"
            },
            "public.some_table.some_profit_sum_xxx": {
                key: "some_profit_sum_xxx",
                type: "numeric",
                "default": "0"
            },
            "public.some_table.some_profit": {
                key: "some_profit",
                type: "numeric",
                "default": "0"
            }
        });
    });

});
