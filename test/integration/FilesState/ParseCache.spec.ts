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

    
});