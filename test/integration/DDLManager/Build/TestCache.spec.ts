import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../../getDbClient";
import { DDLManager } from "../../../../lib/DDLManager";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";

use(chaiShallowDeepEqualPlugin);

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/DDLManager.build cache", () => {
    let db: any;
    
    beforeEach(async() => {
        db = await getDBClient();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });

    afterEach(async() => {
        db.end();
    });

    it("test cache commutative/self update triggers working", async() => {
        const folderPath = ROOT_TMP_PATH + "/universal_cache_test";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table invoices (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                order_number text,
                order_date date
            );
            create table invoice_positions (
                id_order integer,
                id_invoice integer
            );
        `);

        fs.writeFileSync(folderPath + "/orders.sql", `
            cache special_number for orders (
                select
                    -- spec_numb = 'XXX (2021-01-28)'
                    (
                        orders.order_number || coalesce(
                            ' (' || orders.order_date::text || ')',
                            ''
                        )
                    ) collate "POSIX"
                    as spec_numb
            )
        `);

        fs.writeFileSync(folderPath + "/invoice_orders_ids.sql", `
            cache invoice_orders_ids for invoices (
                select
                    array_agg(
                        distinct invoice_positions.id_order
                    ) as orders_ids

                from invoice_positions
                where
                    invoice_positions.id_invoice = invoices.id
            )
        `);

        fs.writeFileSync(folderPath + "/invoices.sql", `
            cache orders_totals for invoices (
                select
                    string_agg(
                        distinct orders.order_number, ', '
                    ) as orders_usual_numbers,

                    string_agg(
                        distinct orders.spec_numb, ', '
                        order by orders.spec_numb
                    )
                    as orders_spec_numbers
                
                from orders
                where
                    orders.id = any( invoices.orders_ids )
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result;


        // test insert
        await db.query(`
            insert into invoices default values;
            insert into orders 
                (order_number)
            values
                ('initial');

            insert into invoice_positions (
                id_invoice,
                id_order
            ) values (
                1,
                1
            );

            update orders set
                order_date = '2020-12-26'::date,
                order_number = 'order-26'
            where id = 1;
        `);
        result = await db.query(`
            select
                id,
                orders_spec_numbers,
                orders_ids,
                orders_usual_numbers

            from invoices
        `);

        assert.deepStrictEqual(result.rows, [{
            id: 1,
            orders_ids: [1],
            orders_usual_numbers: "order-26",
            orders_spec_numbers: "order-26 (2020-12-26)"
        }]);
    });

});