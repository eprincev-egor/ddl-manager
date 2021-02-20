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

    it("test cache with custom agg", async() => {
        const folderPath = ROOT_TMP_PATH + "/max_or_null";
        fs.mkdirSync(folderPath);

        await db.query(`
            create or replace function max_or_null_date(
                prev_date timestamp without time zone,
                next_date timestamp without time zone
            ) returns timestamp without time zone as $body$
            begin
                if prev_date = '0001-01-01 00:00:00' then
                    return next_date;
                end if;

                if prev_date is null then
                    return null;
                end if;

                if next_date is null then
                    return null;
                end if;

                return greatest(prev_date, next_date);
            end
            $body$
            language plpgsql;

            create or replace function max_or_null_date_final(
                final_date timestamp without time zone
            ) returns timestamp without time zone as $body$
            begin
                if final_date = '0001-01-01 00:00:00' then
                    return null;
                end if;
            
                return final_date;
            end
            $body$
            language plpgsql;

            CREATE AGGREGATE max_or_null_date_agg (timestamp without time zone)
            (
                sfunc = max_or_null_date,
                finalfunc = max_or_null_date_final,
                stype = timestamp without time zone,
                initcond = '0001-01-01T00:00:00.000Z'
            );

            create table public.order (
                id serial primary key
            );

            drop schema if exists operation cascade;
            create schema operation;
            create table operation.unit (
                id serial primary key,
                id_order bigint,
                sea_date timestamp without time zone,
                deleted smallint default 0
            );

            insert into public.order default values;
            insert into operation.unit (id_order)
            values (1);
        `);

        fs.writeFileSync(folderPath + "/orders.sql", `
            cache unit_dates for public.order (
                select
                    max_or_null_date_agg( unit.sea_date ) as max_or_null_sea_date
                from operation.unit
                where
                    unit.id_order = public.order.id and
                    unit.deleted = 0
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result;


        // test default values
        await testOrder({
            id: 1,
            max_or_null_sea_date_sea_date: [null],
            max_or_null_sea_date: null
        });

        // test set deleted = 1
        await db.query(`
            update operation.unit set
                deleted = 1
        `);
        await testOrder({
            id: 1,
            max_or_null_sea_date_sea_date: [],
            max_or_null_sea_date: null
        });

        // test insert two units
        await db.query(`
            insert into operation.unit (id_order)
            values (1), (1);
        `);
        await testOrder({
            id: 1,
            max_or_null_sea_date_sea_date: [null, null],
            max_or_null_sea_date: null
        });


        const someDate = "2021-02-20 10:10:10";

        // test update first unit
        await db.query(`
            update operation.unit set
                sea_date = '${someDate}'
            where
                id = 2
        `);
        result = await db.query(`
            select
                id,
                max_or_null_sea_date_sea_date::text[],
                max_or_null_sea_date
            from public.order
        `);
        await testOrder({
            id: 1,
            max_or_null_sea_date_sea_date: [null, someDate],
            max_or_null_sea_date: null
        });


        // test update second unit
        await db.query(`
            update operation.unit set
                sea_date = '${someDate}'
            where
                id = 3
        `);
        await testOrder({
            id: 1,
            max_or_null_sea_date_sea_date: [someDate, someDate],
            max_or_null_sea_date: someDate
        });

        // test insert third unit
        await db.query(`
            insert into operation.unit (id_order)
            values (1)
        `);
        await testOrder({
            id: 1,
            max_or_null_sea_date_sea_date: [someDate, someDate, null],
            max_or_null_sea_date: null
        });
            
        // test trash and update second unit
        const otherDate = "2021-03-10 20:20:20";
        await db.query(`
            update operation.unit set
                deleted = 1
            where
                id = 3
        `);
        await db.query(`
            update operation.unit set
                sea_date = '${otherDate}'
            where
                id = 3
        `);
        await testOrder({
            id: 1,
            max_or_null_sea_date_sea_date: [someDate, null],
            max_or_null_sea_date: null
        });


        async function testOrder(expectedRow: {
            id: number,
            max_or_null_sea_date_sea_date: (string | null)[],
            max_or_null_sea_date: string | null
        }) {
            result = await db.query(`
                select
                    id,
                    max_or_null_sea_date_sea_date::text[],
                    max_or_null_sea_date::text
                from public.order
            `);
            assert.deepStrictEqual(result.rows, [expectedRow]);
        }
    });

});