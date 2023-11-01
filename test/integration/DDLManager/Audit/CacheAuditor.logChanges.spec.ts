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

describe("CacheAuditor: create/recreate logger for broken columns", () => {
    
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

        it("need recreate changes columns", async() => {
            await db.query(`
                create table ddl_manager_audit_column_changes (
                    id bigserial primary key
                );
            `);
            await audit();
            
            await db.query(`
                update companies set
                    some_column = 0
                where id = 1;
            `);

            await expectedChanges("changed_new_row", {
                id: 1,
                some_column: 0
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

        it("log update of cache column", async() => {
            await db.query(`
                update companies set
                    orders_numbers = 'wrong'
                where id = 1
            `);
            
            await expectedChanges("changed_old_row", {
                id: 1,
                orders_numbers: "order-1, order-2"
            });
            await expectedChanges("changed_new_row", {
                id: 1,
                orders_numbers: "wrong"
            });
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

            it("log source changes (need columns for cache only)", async () => {
                await expectedChanges("changed_old_row", {
                    id: 3,
                    id_client: 2,
                    doc_number: "order-3",
                    profit: 300
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

            it("log source changes (need columns for cache only)", async () => {
                await expectedChanges("changed_old_row", {
                    id: 1,
                    id_client: 1,
                    doc_number: "order-1",
                    profit: 100
                });
                await expectedChanges("changed_new_row", {
                    id: 1,
                    id_client: 1,
                    doc_number: "update order 1",
                    profit: 100
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

    describe("many broken columns per table", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/self.sql", `
                cache self for companies (
                    select
                        companies.id - 1 as index,
                        coalesce(companies.name, '') as not_null_name
                )
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/profit.sql", `
                cache profit for companies (
                    select
                        sum( orders.profit ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/orders_numbers.sql", `
                cache orders_numbers for companies (
                    select
                        string_agg( orders.doc_number, ', ' ) as orders_numbers
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
            await build();

            await db.query(`
                update companies set
                    index = -1,
                    orders_profit = 666
            `);
            await audit();
        });

        it("log update agg source table", async() => {
            await db.query(`
                update orders set
                    profit = 303
                where id = 3
            `);
            
            const lastChanges = await loadLastColumnChanges();
            strict.ok(
                lastChanges.cache_columns
                    .includes("public.companies.orders_profit"),
                "cache_columns includes orders_profit"
            );
            strict.ok(
                lastChanges.cache_columns
                    .includes("public.companies.orders_numbers"),
                "cache_columns includes orders_numbers"
            );
        });

        it("log update self cache table", async() => {
            await db.query(`
                update companies set
                    name = 'test'
                where id = 1
            `);
            
            const lastChanges = await loadLastColumnChanges();
            strict.ok(
                lastChanges.cache_columns
                    .includes("public.companies.index"),
                "cache_columns includes index"
            );
            strict.ok(
                lastChanges.cache_columns
                    .includes("public.companies.not_null_name"),
                "cache_columns includes not_null_name"
            );
        });
    });

    describe("cache with array reference", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/payments.sql", `
                cache payments for orders (
                    select
                        array_agg( distinct link.id_payment ) as payments_ids
                    from order_payment_link as link
                    where
                        link.id_order = orders.id
                )
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/orders.sql", `
                cache orders for payments (
                    select
                        string_agg( distinct orders.doc_number, ', ' ) as orders_numbers
                    from orders
                    where
                        orders.payments_ids && array[ payments.id ]
                )
            `);
            await build();

            await db.query(`
                insert into payments default values;
                insert into payments default values;

                insert into order_payment_link (
                    id_order,
                    id_payment,
                    part_of_payment
                ) values
                    (1, 1, 100),
                    (2, 1, 100)
                ;

                update payments set
                    orders_numbers = 'wrong'
            `);
            await audit();
        });

    });

    describe("cache with mutable order by", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/totals.sql", `
                cache totals for companies (
                    select
                        orders.doc_number as best_order_number
                    from orders
                    where
                        orders.id_client = companies.id

                    order by orders.profit desc
                    limit 1
                )
            `);
            await build();

            await db.query(`
                update companies set
                    best_order_number = 'wrong'
            `);
            await audit();
        });

        it("log on update sort column", async () => {
            await db.query(`
                update orders set
                    profit = 350
                where id = 1;
            `);

            await expectedChanges("changed_old_row", {
                id: 1,
                id_client: 1,
                doc_number: "order-1",
                profit: 100,
                __totals_for_companies: false // private field, TODO: remove from test
            });
            await expectedChanges("changed_new_row", {
                id: 1,
                id_client: 1,
                doc_number: "order-1",
                profit: 350,
                __totals_for_companies: false // private field, TODO: remove from test
            });
        });
    });

    describe("cache same table deps, but other rows", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/parent.sql", `
                cache parent for companies as child_company (
                    select
                        parent_company.name as parent_company_name
                    from companies as parent_company
                    where
                        parent_company.id = child_company.id_parent
                )
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/children.sql", `
                cache children for companies as parent_company (
                    select
                        string_agg(child_company.name, ', ') as children_companies_names
                    from companies as child_company
                    where
                        child_company.id_parent = parent_company.id
                )
            `);
            await build();

            await db.query(`
                update companies set
                    parent_company_name = 'wrong',
                    children_companies_names = 'wrong'
            `);
            await audit();

            await db.query(`
                update companies set
                    id_parent = 1
                where id = 2;
            `);
        });

        it("log cache values", async () => {
            await expectedChanges("changed_old_row", {
                id: 2,
                id_parent: null,
                name: "partner",
                parent_company_name: null,
                children_companies_names: null,
                __children_json__: null // private field, TODO: remove from test
            });
            await expectedChanges("changed_new_row", {
                id: 2,
                id_parent: 1,
                name: "partner",
                parent_company_name: "client",
                children_companies_names: null,
                __children_json__: null // private field, TODO: remove from test
            });
        });

        it("log cache_columns", async () => {
            const lastChanges = await loadLastColumnChanges();
            strict.ok(
                lastChanges.cache_columns
                    .includes("public.companies.children_companies_names"),
                "cache_columns includes children_companies_names"
            );
            strict.ok(
                lastChanges.cache_columns
                    .includes("public.companies.parent_company_name"),
                "cache_columns includes parent_company_name"
            );
        });
    });

    describe("recursion cache", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/lvl.sql", `
                cache lvl for companies (
                    select
                        coalesce(
                            companies.parent_lvl + 1,
                            1
                        )::integer as lvl
                )
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/parent.sql", `
                cache parent for companies as child_company (
                    select
                    -- parent_lvl служебное поле необходимое для работы обычного lvl
                        parent_company.lvl as parent_lvl
                
                    from companies as parent_company
                    where
                        parent_company.id = child_company.id_parent
                )
            `);
            await build();

            await db.query(`
                update companies set
                    lvl = -1
            `);
            await audit();
        });

        it("log on update reference column", async () => {
            await db.query(`
                update companies set
                    id_parent = 1
                where id = 2;
            `);

            await expectedChanges("changed_old_row", {
                id: 2,
                id_parent: null,
                parent_lvl: null,
                lvl: 1
            });
            await expectedChanges("changed_new_row", {
                id: 2,
                id_parent: 1,
                parent_lvl: 1,
                lvl: 2
            });
        });
    });

    describe("cache dependent on custom before update trigger", () => {
        beforeEach(async () => {
            fs.writeFileSync(ROOT_TMP_PATH + "/totals.sql", `
                cache totals for companies (
                    select
                        companies.name || companies.note as name_note
                )
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/custom.sql", `
                create or replace function set_note()
                returns trigger as $body$
                begin
                    new.note = new.id_parent::text;
                    return new;
                end
                $body$ language plpgsql;

                create trigger a_test 
                before update of id_parent
                on companies
                for each row
                execute procedure set_note();
            `);
            await build();

            await db.query(`
                update companies set
                    name_note = 'wrong'
            `);
            await audit();
        });

        it("log update", async () => {
            await db.query(`
                update companies set
                    id_parent = 1
                where id = 1;
            `);

            await expectedChanges("changed_old_row", {
                id: 1,
                name: "client",
                note: null,
                name_note: null
            });
            await expectedChanges("changed_new_row", {
                id: 1,
                name: "client",
                note: "1",
                name_note: "client1"
            });
        });
    });

    describe("remove logger", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        companies.name || 'test' as note
                )
            `);
            await build();

            await db.query(`
                update companies set
                    note = 'wrong'
            `);
            await audit();
        });

        it("remove logger if no more cache", async () => {
            fs.unlinkSync(ROOT_TMP_PATH + "/cache.sql");

            await audit();

            await db.query(`
                update companies set
                    note = 'wrong 2'
            `);
            const lastChanges = await loadLastColumnChanges();
            strict.equal(lastChanges, undefined);
        });

        it("remove logger if long time no reports", async () => {
            await db.query(`
                update ddl_manager_audit_report set
                    scan_date = now() - interval '3 months 5 days'
            `);

            await audit();

            await db.query(`
                update companies set
                    note = 'wrong 2'
            `);
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

    async function expectedChanges(column: string, expected: any) {
        const lastChanges = await loadLastColumnChanges();
        strict.ok(lastChanges, "changes should be saved");

        const actual = lastChanges[column];
        strict.deepEqual({
            [column]: actual
        }, {
            [column]: (
                isObject(expected) ?
                    {...actual, ...expected} :
                    expected
            )
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

    function isObject(value: any) {
        return (
            value &&
            typeof value === "object" &&
            !Array.isArray(value)
        )
    }
})