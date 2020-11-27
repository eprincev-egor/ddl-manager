import fs from "fs";
import fse from "fs-extra";
import { FilesState } from "../../../lib/FilesState";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import assert from "assert";

use(chaiShallowDeepEqualPlugin);

describe("integration/FilesState parse cache", () => {
    const ROOT_TMP_PATH = __dirname + "/tmp";
    
    beforeEach(() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });
    
    afterEach(() => {
        fse.removeSync(ROOT_TMP_PATH);
    });

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

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getCache()).to.be.shallowDeepEqual([
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
            FilesState.create({
                folder: ROOT_TMP_PATH
            });
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
            FilesState.create({
                folder: ROOT_TMP_PATH
            });
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
            FilesState.create({
                folder: ROOT_TMP_PATH
            });
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
            FilesState.create({
                folder: ROOT_TMP_PATH
            });
        }, (err: Error) =>
            /GROUP BY are not supported/
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
            FilesState.create({
                folder: ROOT_TMP_PATH
            });
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
            FilesState.create({
                folder: ROOT_TMP_PATH
            });
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
            FilesState.create({
                folder: ROOT_TMP_PATH
            });
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
            FilesState.create({
                folder: ROOT_TMP_PATH
            });
        }, (err: Error) =>
            /duplicated cache column companies\.orders_profit/
                .test(err.message)
        );
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
            FilesState.create({
                folder: ROOT_TMP_PATH
            });
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
            FilesState.create({
                folder: ROOT_TMP_PATH
            });
        }, (err: Error) =>
            /required select any columns or expressions/
                .test(err.message)
        );
    });

});