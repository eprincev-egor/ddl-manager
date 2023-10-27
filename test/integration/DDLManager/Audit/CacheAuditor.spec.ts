import { Pool } from "pg";
import fs from "fs";
import {prepare, buildDDL} from "./fixture";
import { CacheAuditor, CacheScanner } from "../../../../lib/Auditor";
import { FileReader } from "../../../../lib/fs/FileReader";
import { Database } from "../../../../lib/database/schema/Database";
import { PostgresDriver } from "../../../../lib/database/PostgresDriver";
import { CacheColumnGraph } from "../../../../lib/Comparator/graph/CacheColumnGraph";
import { deepEqualRow, shouldBeBetween } from "./utils";
import { strict } from "assert";
import { DDLManager } from "../../../../lib/DDLManager";

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("CacheAuditor", () => {
    
    let db: Pool;
    beforeEach(async () => {
        db = await prepare();
    });

    describe("audit simple broken cache", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        1 as some_column
                )
            `);
            await build();

            await db.query(`
                update companies set
                    some_column = 0
            `);
        });

        it("should fix data", async () => {
            await audit();
            
            const result = await db.query(`
                select id, some_column 
                from companies
                order by id
            `);
            strict.deepEqual(result.rows, [
                {id: 1, some_column: 1},
                {id: 2, some_column: 1}
            ]);
        });

        describe("should save report", async () => { 

            it("save schema, table, column", async() => {
                await audit();

                await expectedReport("cache_column", "public.companies.some_column");
            });

            it("save expected, actual cache value", async() => {
                await audit();

                await expectedReport("expected_value", 1);
                await expectedReport("actual_value", 0);
            });
            
            it("save scan date", async() => {
                const dateStart = new Date();
                await audit();
                const dateEnd = new Date();

                const lastReport = await loadLastReport();
                shouldBeBetween(lastReport.scan_date, dateStart, dateEnd);
            });
            
            it("save cache row", async() => {
                await audit();

                const lastReport = await loadLastReport();
                deepEqualRow(lastReport.cache_row, {
                    id: 1,
                    name: "client",
                    some_column: 0
                });
            });
        });

    });

    describe("log changes for agg cache", async () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        string_agg( distinct orders.doc_number, ', ' ) as orders_numbers,
                        sum( orders.profit ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
            await build();

            await db.query(`
                update companies set
                    orders_numbers = 'wrong'
            `);
            await audit();
        });


        it("log callstack", async () => {
            await db.query(`
                create or replace function inner_func()
                returns void as $body$
                begin
                    -- some code
                    -- here

                    update orders set
                        doc_number = 'some new value';
                end
                $body$ language plpgsql;

                create or replace function main_func()
                returns void as $body$
                begin
                    perform inner_func();
                end
                $body$ language plpgsql;

                select main_func();
            `);

            await expectedChanges("callstack", [
                "inner_func:6",
                "main_func:3"
            ]);
        });

        it("log entry query", async() => {
            const sql = `/* test */update orders set doc_number = 'test!'/* test */`;
            await db.query(sql);
            
            await expectedChanges("entry_query", sql);
        });

        describe("log insert into source table", () => {
            beforeEach(async () => {
                await db.query(`
                    insert into orders (id_client, doc_number)
                    values (2, 'order-3');
                `);
            });

            it("log TG_OP", async () => {
                await expectedChanges("changes_type", "INSERT");
            });

            it("log source table", async () => {
                await expectedChanges("changed_table", "public.orders");
            });

            it("log source row id", async () => {
                await expectedChanges("changed_row_id", "4");
            });

            it("log cache rows ids", async () => { 
                await expectedChanges("cache_rows_ids", ["2"]);
            });

            it("log source changes (need columns for cache only)", async () => {
                await expectedChanges("changed_old_row", null);
                await expectedChanges("changed_new_row", {
                    id: 4,
                    id_client: 2,
                    doc_number: "order-3",
                    profit: null
                });
            });
        });

        describe("log delete from source table", () => {
            beforeEach(async () => {
                await db.query(`
                    delete from orders 
                    where id_client = 2;
                `);
            });

            it("log TG_OP", async () => {
                await expectedChanges("changes_type", "DELETE");
            });

            it("log source table", async () => {
                await expectedChanges("changed_table", "public.orders");
            });

            it("log source row id", async () => {
                await expectedChanges("changed_row_id", "3");
            });

            it("log cache rows ids", async () => { 
                await expectedChanges("cache_rows_ids", ["2"]);
            });

            it("log source changes (need columns for cache only)", async () => {
                await expectedChanges("changed_old_row", {
                    id: 3,
                    id_client: 2,
                    doc_number: "order-3",
                    profit: null
                });
                await expectedChanges("changed_new_row", null);
            });
        });

        it("ignore update not cache columns", async() => {
            await db.query(`update orders set note = 'test'`);
            
            const lastChanges = await loadLastColumnChanges();
            strict.equal(lastChanges, undefined);
        });

        describe("log update of source column", () => {
            beforeEach(async () => {
                await db.query(`
                    update orders set
                        doc_number = 'update order 1'
                    where id = 1
                `);
            });

            it("log TG_OP", async () => {
                await expectedChanges("changes_type", "UPDATE");
            });

            it("log source table", async () => {
                await expectedChanges("changed_table", "public.orders");
            });

            it("log source row id", async () => {
                await expectedChanges("changed_row_id", "1");
            });

            it("log cache columns", async () => {
                const lastChanges = await loadLastColumnChanges();
                const cacheColumns: string[] = lastChanges?.cache_columns || [];

                strict.ok(cacheColumns.includes(
                    "public.companies.orders_numbers"
                ), "has orders_numbers");

                strict.ok(cacheColumns.includes(
                    "public.companies.orders_profit"
                ), "has orders_profit");
            });

            it("log cache rows ids", async () => { 
                await expectedChanges("cache_rows_ids", ["1"]);
            });

            it("log source changes (need columns for cache only)", async () => {
                await expectedChanges("changed_old_row", {
                    id: 1,
                    id_client: 1,
                    doc_number: "order-1",
                    profit: null
                });
                await expectedChanges("changed_new_row", {
                    id: 1,
                    id_client: 1,
                    doc_number: "update order 1",
                    profit: null
                });
            });

            it("log changes date", async () => {
                const dateStart = new Date();
                await db.query(`
                    update orders set
                        doc_number = 'order A'
                    where id = 1
                `);
                const dateEnd = new Date();

                const lastChanges = await loadLastColumnChanges();
                shouldBeBetween(lastChanges.changes_date, dateStart, dateEnd);
                shouldBeBetween(lastChanges.transaction_date, dateStart, dateEnd);
            });

        });

    });

    describe("log self row cache", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        '#' || companies.id || 
                        coalesce(' ' || companies.name, '') as ref_name
                )
            `);
            await build();

            await db.query(`
                update companies set
                    ref_name = 'wrong'
            `);
            await audit();
        });

        it("log update of cache row", async () => {
            await db.query(`
                update companies set
                    name = 'new name'
                where id = 1;
            `);

            await expectedChanges("changed_old_row", {
                id: 1,
                name: "client",
                ref_name: "#1 client"
            });
            await expectedChanges("changed_new_row", {
                id: 1,
                name: "new name",
                ref_name: "#1 new name"
            });
        });

        it("log insert of cache row", async () => {
            await db.query(`
                insert into companies (name)
                values ('test');
            `);

            await expectedChanges("changes_type", "INSERT");
            await expectedChanges("changed_new_row", {
                id: 3,
                name: "test",
                ref_name: "#3 test"
            });
        });

        it("log delete of cache row", async () => {
            await db.query(`
                delete from companies
                where id = 1;
            `);

            await expectedChanges("changes_type", "DELETE");
            await expectedChanges("changed_old_row", {
                id: 1,
                name: "client",
                ref_name: "#1 client"
            });
        });

        it("ignore update-ddl-cache operation (big migration)", async() => {
            await db.query(`
                alter table companies disable trigger all;
                update companies set
                    ref_name = null;
                alter table companies enable trigger all;
            `);
            await DDLManager.refreshCache({
                db,
                folder: ROOT_TMP_PATH,
                throwError: true,
                needLogs: false
            });
            
            const lastChanges = await loadLastColumnChanges();
            strict.equal(lastChanges, undefined);
        });

    });

    async function build() {
        await buildDDL(db);
    }

    async function audit() {
        const fsState = FileReader.read([ROOT_TMP_PATH]);
        const dbState = new Database();
        const graph = CacheColumnGraph.build(
            new Database().aggregators,
            fsState.allCache()
        );
        const postgres = new PostgresDriver(db);

        const scanner = new CacheScanner(
            postgres,
            dbState,
            graph
        );
        const auditor = new CacheAuditor(
            postgres,
            dbState,
            fsState,
            graph,
            scanner,
        );

        await auditor.audit();
    }

    async function expectedReport(column: string, expected: any) {
        const lastReport = await loadLastReport();

        strict.ok(lastReport, "report should be saved");
        strict.deepEqual({
            [column]: lastReport[column]
        }, {
            [column]: expected
        });
    }

    async function loadLastReport() {
        const {rows} = await db.query(`
            select
                *,
                to_char(
                    ddl_manager_audit_report.scan_date at time zone 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
                ) as scan_date
            from ddl_manager_audit_report
            order by ddl_manager_audit_report.id desc
            limit 1
        `);
        return rows[0];
    }

    async function expectedChanges(column: string, expected: any) {
        const lastChanges = await loadLastColumnChanges();

        strict.ok(lastChanges, "changes should be saved");
        strict.deepEqual({
            [column]: lastChanges[column]
        }, {
            [column]: expected
        });
    }

    async function loadLastColumnChanges() {
        const {rows} = await db.query(`
            select
                *,
                to_char(
                    ddl_manager_audit_column_changes.transaction_date at time zone 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
                ) as transaction_date,
                to_char(
                    ddl_manager_audit_column_changes.changes_date at time zone 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
                ) as changes_date

            from ddl_manager_audit_column_changes
            order by ddl_manager_audit_column_changes.id desc
            limit 1
        `);
        return rows[0];
    }
})