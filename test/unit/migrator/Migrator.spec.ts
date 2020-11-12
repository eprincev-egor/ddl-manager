import assert from "assert";
import { FakeDatabase } from "./FakeDatabase";
import { Migrator } from "../../../lib/Migrator";
import { Diff } from "../../../lib/Diff";
import { Cache, DatabaseFunction, DatabaseTrigger } from "../../../lib/ast";
import { IState } from "../../../lib/interface";
import { FileParser } from "../../../lib/parser";


describe("Migrator", () => {

    let database!: FakeDatabase;
    let changes!: Diff;
    beforeEach(() => {
        database = new FakeDatabase();
        changes = Diff.empty();
    });

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

        changes.createState({cache: [generateTestCache()]});

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

        const cache = generateTestCache()
        changes.createState({cache: [cache]});

        database.setRowsCount(cache.for.table.toString(), 1499);

        await Migrator.migrate(database, changes);

        assert.deepStrictEqual(
            database.getUpdatedPackages(cache.for.table.toString()),
            [{limit: 500}, {limit: 500}, {limit: 500}]
        );
    });

    function generateTestCache() {
        const fileContent = FileParser.parse(`
            cache test for some_table (
                select
                    sum( another_table.profit ) as orders_profit
                from another_table
                where
                    another_table.id_client = some_table.id
            )
        `) as IState;
        const testCache = (fileContent.cache as Cache[])[0] as Cache;

        return testCache;
    }
});
