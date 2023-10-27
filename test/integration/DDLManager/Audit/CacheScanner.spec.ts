import { Pool } from "pg";
import fs from "fs";
import { createScanner, prepare, ROOT_TMP_PATH, buildDDL } from "./fixture";
import { deepEqualRows } from "./utils";
import { strict } from "assert";
import { IColumnScanResult, IFindBrokenColumnsParams } from "../../../../lib/Auditor";

describe("CacheScanner", () => {

    let db: Pool;
    beforeEach(async () => {
        db = await prepare();
    });

    describe("scan simple cache without errors, now wrong values", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        1 as some_column
                )
            `);
            await build();
        });

        it("should return column", async () => {
            const result = await scan();
            strict.equal(result.column, "public.companies.some_column");
        });

        it("should return 'no wrong values' if not found wrong rows", async () => {
            const result = await scan();
            strict.equal(result.hasWrongValues, false);
        });

        it("should return time duration", async () => {
            const result = await scan();
            strict.ok(result.time.duration >= 0);
        });

        it("should return time start", async () => {
            const result = await scan();
            strict.ok(result.time.start instanceof Date);
        });

        it("should return time end", async () => {
            const result = await scan();
            strict.ok(result.time.end instanceof Date);
        });

        it("time end should be great than time start", async () => {
            const result = await scan();
            strict.ok(+result.time.end >= +result.time.start);
        });
    });

    describe("caching error on scan simple cache", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        1 as some_column
                )
            `);
            await build();

            await db.query(`
                alter table companies
                    drop column some_column cascade
            `);
        });

        it("should throw correct error message", async () => {
            await strict.rejects(
                scan(),
                /column companies.some_column does not exist/
            );
        });

        it("should throw column", async () => {
            await strict.rejects(
                scan(), error => 
                    error.event.column === "public.companies.some_column"
            );
        });

        it("should throw time duration", async () => {
            await strict.rejects(
                scan(), error => 
                    error.event.time.duration >= 0
            );
        });

        it("should return time start", async () => {
            await strict.rejects(
                scan(), error => 
                    error.event.time.start instanceof Date
            );
        });

        it("should return time end", async () => {
            await strict.rejects(
                scan(), error => 
                    error.event.time.end instanceof Date
            );
        });

        it("time end should be great than time start", async () => {
            await strict.rejects(
                scan(), error => 
                    +error.event.time.end >= +error.event.time.start
            );
        });
    });

    describe("scan simple cache with wrong values", () => {
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

        it("should return 'has wrong values' if found wrong rows", async () => {
            const result = await scan();
            strict.equal(result.hasWrongValues, true);
        });

        it("should return table", async () => {
            const result = await scan();
            strict.equal(result.wrongExample?.table, "public.companies");
        });

        it("should return select for expected", async () => {
            const result = await scan();
            strict.ok(result.wrongExample?.selectExpectedForThatRow);
        });

        it("should return expected value", async () => {
            const result = await scan();
            strict.equal(result.wrongExample?.expected, 1);
        });

        it("should return actual value", async () => {
            const result = await scan();
            strict.equal(result.wrongExample?.actual, 0);
        });

        it("should return wrong row id", async () => {
            const result = await scan();
            strict.equal(result.wrongExample?.cacheRow.id, 1);
        });

        it("should return other fields of broken row", async () => {
            const result = await scan();
            strict.equal(result.wrongExample?.cacheRow.name, "client");
        });
    });

    describe("scan cache with string_agg", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        string_agg( distinct orders.doc_number, ', ' ) as orders_numbers
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
            await build();
        });

        it("ignore case when cache row contains strings in order: (1, 2)", async () => {
            await db.query(`
                update companies set
                    orders_numbers = 'order-2, order-1'
                where id = 1;
            `);

            const result = await scan();
            strict.equal(result.hasWrongValues, false);
        });

        it("ignore case when cache row contains strings in order: (2, 1)", async () => {
            await db.query(`
                update companies set
                    orders_numbers = 'order-1, order-2'
                where id = 1;
            `);

            const result = await scan();
            strict.equal(result.hasWrongValues, false);
        });

        it("should return source orders", async () => {
            await db.query(`
                update companies set
                    orders_numbers = null
                where id = 1;
            `);

            const result = await scan();

            deepEqualRows(result.wrongExample?.sourceRows, [
                {id: 1, id_client: 1, doc_number: "order-1"},
                {id: 2, id_client: 1, doc_number: "order-2"}
            ]);
        });
    });

    describe("scan cache with array_agg", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        array_agg( distinct orders.doc_number) as orders_numbers
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
            await build();
        });

        it("ignore case when cache row contains strings in order: (1, 2)", async () => {
            await db.query(`
                update companies set
                    orders_numbers = ARRAY['order-2', 'order-1']
                where id = 1;
            `);

            const result = await scan();
            strict.equal(result.hasWrongValues, false);
        });

        it("ignore case when cache row contains strings in order: (2, 1)", async () => {
            await db.query(`
                update companies set
                    orders_numbers = ARRAY['order-1', 'order-2']
                where id = 1;
            `);

            const result = await scan();
            strict.equal(result.hasWrongValues, false);
        });
    });

    describe("scan cache with sum", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        sum( orders.profit ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
            await build();
        });

        it("ignore 0 != null", async () => {
            await db.query(`
                update orders set
                    profit = null;

                update companies set
                    orders_profit = 0
            `);

            const result = await scan();
            strict.equal(result.hasWrongValues, false);
        });

        it("ignore null != 0", async () => {
            await db.query(`
                update orders set
                    profit = 0;

                update companies set
                    orders_profit = null
            `);

            const result = await scan();
            strict.equal(result.hasWrongValues, false);
        });

        it("helper columns can be broken", async () => {
            await db.query(`
                alter table orders disable trigger all;
                update orders set
                    profit = 100
            `);

            const result = await scan();
            strict.equal(result.hasWrongValues, true);
        })
    });

    describe("scan cache one row (like are left join)", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for orders (
                    select
                        companies.name as client_name
                    from companies
                    where
                        companies.id = orders.id_client
                )
            `);
            await build();
        });

        it("should return source company", async () => {
            await db.query(`
                update orders set
                    client_name = 'wrong'
                where id = 1;
            `);

            const result = await scan();

            deepEqualRows(result.wrongExample?.sourceRows, [
                {id: 1, name: "client"}
            ]);
        });
    });

    describe("scan cache one last row", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        orders.doc_number as last_order_number
                    from orders
                    where
                        orders.id_client = companies.id

                    order by orders.id desc
                    limit 1
                )
            `);
            await build();
        });

        it("should return source orders", async () => {
            await db.query(`
                update companies set
                    last_order_number = 'wrong'
                where id = 1;
            `);

            const result = await scan();

            deepEqualRows(result.wrongExample?.sourceRows, [
                {id: 1, id_client: 1, doc_number: "order-1"},
                {id: 2, id_client: 1, doc_number: "order-2"}
            ]);
        });

    });

    // TODO: source rows filter by reference only

    async function build() {
        await buildDDL(db);
    }

    async function scan(
        params: IFindBrokenColumnsParams = {}
    ): Promise<IColumnScanResult> {
        const scanner = createScanner(db);

        const scans: IColumnScanResult[] = [];
        return new Promise((resolve, reject) => {
            scanner.scan({
                ...params,
                onScanColumn(event) {
                    scans.push(event);
                },
                onFinish() {
                    resolve(scans[0]);
                },
                onScanError(event) {
                    const error = new Error(event.error.message) as any;
                    error.event = event;
                    reject(error);
                }
            });
        });
    }

})