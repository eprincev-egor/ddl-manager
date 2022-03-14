import fs from "fs";
import { flatMap } from "lodash";
import { FileReader } from "../../../lib/fs/FileReader";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import assert from "assert";
import { prepare } from "../utils/prepare";
import { CacheIndex } from "../../../lib/ast/CacheIndex";

use(chaiShallowDeepEqualPlugin);

describe("integration/FileReader parse cache", () => {

    const ROOT_TMP_PATH = __dirname + "/tmp";
    prepare(ROOT_TMP_PATH);

    it("parse file with simple cache", () => {

        const sql = `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        const state = FileReader.read([ROOT_TMP_PATH]);

        expect(flatMap(state.files, file => file.content.cache)).to.be.shallowDeepEqual([
            {
                name: "totals",
                for: {
                    table: {
                        schema: "public",
                        name: "companies"
                    },
                    as: undefined
                },
                select: {
                    columns: [
                        {
                            expression: {elements: [
                                {name: "sum", args: [
                                    {elements: [
                                        {tableReference: {
                                            table: {
                                                schema: "public",
                                                name: "orders"
                                            },
                                            as: undefined
                                        }, name: "profit"}
                                    ]}
                                ]}
                            ]},
                            name: "orders_profit"
                        }
                    ],
                    from: [{
                        table: {
                            table: {
                                schema: "public",
                                name: "orders"
                            },
                            as: undefined
                        },
                        joins: []
                    }],
                    where: {elements: [
                        {tableReference: {
                            table: {
                                schema: "public",
                                name: "orders"
                            },
                            as: undefined
                        }, name: "id_client"},
                        {operator: "="},
                        {tableReference: {
                            table: {
                                schema: "public",
                                name: "companies"
                            },
                            as: undefined
                        }, name: "id"}
                    ]}
                }
            }
        ]);
    });

    it("cache name for table should be unique", () => {

        const sql1 = `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();

        const sql2 = `
            cache totals for companies (
                select
                    sum( invoices.profit ) as invoices_profit
                from invoices
                where
                    invoices.id_payer = companies.id
            )
        `.trim();
        
        const filePath1 = ROOT_TMP_PATH + "/test-file-1.sql";
        fs.writeFileSync(filePath1, sql1);

        const filePath2 = ROOT_TMP_PATH + "/test-file-2.sql";
        fs.writeFileSync(filePath2, sql2);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /duplicate cache totals for companies/
                .test(err.message)
        );
    });

    
    it("sub queries are not supported (from (sub query))", () => {

        const sql = `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit
                from (
                    select *
                    from orders
                    where
                        orders.id_client = companies.id
                ) as orders
            )
        `.trim();

        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /SUB QUERIES are not supported/
                .test(err.message)
        );
    });

    it("sub queries are not supported (select (sub query))", () => {

        const sql = `
            cache totals for companies (
                select
                    sum(
                        orders.profit + 
                        (select id from order_type limit 1) 
                    ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();

        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /SUB QUERIES are not supported/
                .test(err.message)
        );
    });

    it("GROUP BY are not supported", () => {

        const sql = `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
                group by orders.id_client
            )
        `.trim();

        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /GROUP BY are not supported/
                .test(err.message)
        );
    });

    it("syntax error on string_agg without delimiter", () => {

        const sql = `
            cache totals for orders (
                select
                    string_agg( invoice.doc_number ) || 'test' as invoices_numbers
                from invoice
                where
                    invoice.id_order = orders.id
            )
        `.trim();

        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /required delimiter for string_agg, column: invoices_numbers/
                .test(err.message)
        );
    });

    it("CTE are not supported", () => {

        const sql = `
            cache totals for companies (
                with totals as (select 1)
                select
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();

        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /CTE \(with queries\) are not supported/
                .test(err.message)
        );
    });


    it("UNION are not supported", () => {

        const sql = `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
                union
                select 1
            )
        `.trim();

        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /UNION are not supported/
                .test(err.message)
        );
    });

    it("required alias for columns", () => {

        const sql = `
            cache totals for companies (
                select
                    sum( orders.profit )
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();

        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /required alias for every cache column: sum\(orders\.profit\)/
                .test(err.message)
        );
    });

    it("duplicated cache column name inside one file", () => {

        const sql = `
            cache totals for companies (
                select
                    sum( orders.debet ) as orders_profit,
                    sum( orders.credit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();

        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /duplicated cache column companies\.orders_profit/
                .test(err.message)
        );
    });

    it("duplicated cache column name inside two files", () => {

        const sql1 = `
            cache totals1 for companies (
                select
                    sum( orders.debet ) as my_column
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();
        const sql2 = `
            cache totals2 for companies (
                select
                    sum( orders.credit ) as my_column
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();

        const filePath1 = ROOT_TMP_PATH + "/test-file-1.sql";
        fs.writeFileSync(filePath1, sql1);

        const filePath2 = ROOT_TMP_PATH + "/test-file-2.sql";
        fs.writeFileSync(filePath2, sql2);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /duplicated columns: my_column by cache: totals2, totals1/
                .test(err.message)
        );
    });

    it("parsing without errors two files with cache", () => {

        const sql1 = `
            cache totals1 for companies (
                select
                    sum( orders.debet ) as my_column1
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();
        const sql2 = `
            cache totals2 for companies (
                select
                    sum( orders.credit ) as my_column2
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();

        const filePath1 = ROOT_TMP_PATH + "/test-file-1.sql";
        fs.writeFileSync(filePath1, sql1);

        const filePath2 = ROOT_TMP_PATH + "/test-file-2.sql";
        fs.writeFileSync(filePath2, sql2);

        const state = FileReader.read([ROOT_TMP_PATH]);
        assert.strictEqual(state.files.length, 2);
    });

    it("duplicated cache column name inside one files", () => {

        const sql1 = `
            cache totals1 for companies (
                select
                    sum( orders.debet ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();

        const sql2 = `
            cache totals2 for companies (
                select
                    sum( orders.credit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();
        
        const filePath1 = ROOT_TMP_PATH + "/test-file-1.sql";
        fs.writeFileSync(filePath1, sql1);

        const filePath2 = ROOT_TMP_PATH + "/test-file-2.sql";
        fs.writeFileSync(filePath2, sql2);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /duplicated columns: orders_profit by cache: totals2, totals1/
                .test(err.message)
        );
    });

    it("required cache columns", () => {

        const sql = `
            cache totals for companies (
                select
                from orders
                where
                    orders.id_client = companies.id
            )
        `.trim();

        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, (err: Error) =>
            /required select any columns or expressions/
                .test(err.message)
        );
    });

    it("parse cache with index", () => {

        const sql = `
            cache totals for companies (
                select
                    max( orders.id ) as last_order_id
                from orders
                where
                    orders.id_client = companies.id
            )
            index btree on (last_order_id)
            index btree on (( last_order_id + 1 ))
        `.trim();
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        const state = FileReader.read([ROOT_TMP_PATH]);
        const actualCache = state.files[0].content.cache[0];

        assert.strictEqual(actualCache.indexes.length, 2, "should be two indexes");
        assert.ok( actualCache.indexes[0] instanceof CacheIndex, "first index instance is correct" );
        assert.ok( actualCache.indexes[1] instanceof CacheIndex, "second index instance is correct" );

        assert.deepStrictEqual(
            actualCache.indexes[0].on,
            ["last_order_id"],
            "first index.on is [string]"
        );
        assert.strictEqual(
            actualCache.indexes[1].on.toString(),
            "orders.last_order_id + 1",
            "second index.on is [expression]"
        );
    });

});