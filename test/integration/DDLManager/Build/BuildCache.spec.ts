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
    
    it("build simple cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key,
                orders_profit numeric default 0
            );
            create table orders (
                id serial primary key,
                id_client integer,
                profit numeric
            );

            insert into companies default values;
        `);
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            insert into orders (id_client, profit)
            values (1, 100);
        `);

        const result = await db.query(`
            select orders_profit
            from companies
            where id = 1
        `);
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            orders_profit: 100
        });
    });


    it("create columns for simple cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer,
                profit numeric
            );

            insert into companies default values;
        `);
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            insert into orders (id_client, profit)
            values (1, 100);
        `);

        const result = await db.query(`
            select orders_profit
            from companies
            where id = 1
        `);
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            orders_profit: 100
        });
    });

    it("fill columns for simple cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer,
                profit numeric
            );

            insert into companies default values;
            insert into orders (id_client, profit)
            values (1, 100);
        `);
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select orders_profit
            from companies
            where id = 1
        `);
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            orders_profit: 100
        });
    });

    it("build two cache, with the second dependent on the first", async() => {
        const folderPath = ROOT_TMP_PATH + "/two-caches";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer,
                profit integer
            );
            create table cargos (
                id serial primary key,
                id_order integer,
                gross_weight integer
            );

            insert into companies default values;
            insert into orders (id_client, profit) values (1, 100);
            insert into cargos (id_order, gross_weight) values (1, 150);
        `);
        
        fs.writeFileSync(folderPath + "/orders_cache.sql", `
            cache totals for orders (
                select
                    sum( cargos.gross_weight ) as cargos_gross_weight
                from cargos
                where
                    cargos.id_order = orders.id
            )
        `);
        fs.writeFileSync(folderPath + "/companies_cache.sql", `
            cache totals for companies (
                select
                    sum( orders.cargos_gross_weight ) as cargos_gross_weight,
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select orders_profit, cargos_gross_weight
            from companies
            where id = 1
        `);
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            orders_profit: 100,
            cargos_gross_weight: 150
        });
    });

    it("build cache, with infinity dependencies inside reference", async() => {
        const folderPath = ROOT_TMP_PATH + "/three-caches";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table a (
                id serial primary key,
                value integer,
                id_c integer
            );
            create table b (
                id serial primary key,
                value integer,
                id_a integer
            );
            create table c (
                id serial primary key,
                value integer,
                id_b integer
            );

            insert into a (value, id_c) values (101, 1);
            insert into b (value, id_a) values (102, 1);
            insert into c (value, id_b) values (103, 1);
        `);
        
        fs.writeFileSync(folderPath + "/a.sql", `
            cache totals for a (
                select
                    sum( b.value ) as b_sum
                from b
                where
                    b.id_a = a.id
            )
        `);
        fs.writeFileSync(folderPath + "/b.sql", `
            cache totals for b (
                select
                    sum( c.value ) as c_sum
                from c
                where
                    c.id_b = b.id
            )
        `);
        fs.writeFileSync(folderPath + "/c.sql", `
            cache totals for c (
                select
                    sum( a.value ) as a_sum
                from a
                where
                    a.id_c = c.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result: any;


        result = await db.query(`
            select *
            from a
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            id: 1,
            value: "101",
            b_sum: "102"
        });


        result = await db.query(`
            select *
            from b
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            id: 1,
            value: "102",
            c_sum: "103"
        });


        result = await db.query(`
            select *
            from c
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            id: 1,
            value: "103",
            a_sum: "101"
        });
    });


    it("test cache triggers working", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer,
                profit numeric,
                doc_number text
            );

            insert into companies default values;
        `);
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit,
                    string_agg( distinct orders.doc_number, ', ' ) as orders_doc_numbers,
                    array_agg( distinct orders.profit ) as distinct_profits
                from orders
                where
                    orders.id_client = companies.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result: any;

        await db.query(`
            insert into orders (id_client, profit, doc_number)
            values (1, 100, 'hello');
            insert into orders (id_client, profit, doc_number)
            values (1, 200, 'world');
        `);

        result = await db.query(`
            select orders_profit, orders_doc_numbers, distinct_profits
            from companies
            where id = 1
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            orders_profit: 300,
            orders_doc_numbers: "hello, world",
            distinct_profits: [100, 200]
        });


        await db.query(`
            delete from orders
        `);

        result = await db.query(`
            select orders_profit, orders_doc_numbers, distinct_profits
            from companies
            where id = 1
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            orders_profit: 0,
            orders_doc_numbers: null,
            distinct_profits: null
        });
    });


    it("drop cache columns and triggers", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer,
                profit numeric
            );
        `);
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result;

        result = await db.query(`
            insert into companies default values
            returning *;
        `);

        assert.deepStrictEqual(result.rows[0], {
            id: 1,
            orders_profit: "0"
        });


        fs.unlinkSync(folderPath + "/set_note_trigger.sql");

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        // expected without errors
        await db.query(`
            insert into orders default values;
        `);

        result = await db.query(`
            insert into companies default values
            returning *;
        `);

        assert.deepStrictEqual(result.rows[0], {
            id: 2
        });
    });

    it("default value for cache with count(*)", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer
            );

            insert into companies default values;
        `);
        
        fs.writeFileSync(folderPath + "/set_orders_count.sql", `
            cache totals for companies (
                select
                    count(*) as orders_count
                from orders
                where
                    orders.id_client = companies.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const {rows} = await db.query(`
            insert into companies default values
            returning *
        `);

        assert.deepStrictEqual(rows[0], {
            id: 2,
            orders_count: "0"
        });
    });

    it("test cache with array operators search, check update count", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache-with-array-search";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key,
                orders_numbers text
            );
            create table orders (
                id serial primary key,
                doc_number text,
                deleted integer default 0,
                client_companies_ids integer[]
            );
            create table company_updates (
                company_id integer,
                orders_numbers text
            );

            insert into companies default values;
            insert into companies default values;

            create function log_updates()
            returns trigger as $body$
            begin
                insert into company_updates (company_id, orders_numbers) 
                values (new.id, new.orders_numbers);
                return new;
            end
            $body$
            language plpgsql;

            create trigger log_updates
            after update of orders_numbers
            on companies
            for each row
            execute procedure log_updates();
        `);
        
        fs.writeFileSync(folderPath + "/doc_numbers.sql", `
            cache totals for companies (
                select
                    string_agg(distinct orders.doc_number, ', ') as orders_numbers
                
                from orders
                where
                    orders.client_companies_ids && array[ companies.id ] and
                    orders.deleted = 0
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
            insert into orders (
                doc_number,
                client_companies_ids,
                deleted
            ) 
            values 
                ('order 1', array[1], 0 ),
                ('order 2', array[2], 0 ),
                ('order 3', array[1,2], 0 )
        `);
        result = await db.query(`
            select id, orders_numbers
            from companies
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, orders_numbers: "order 1, order 3"},
            {id: 2, orders_numbers: "order 2, order 3"}
        ], "after insert three orders");

        result = await db.query(`
            select *
            from company_updates
        `);
        assert.deepStrictEqual(result.rows, [
            {company_id: 1, orders_numbers: "order 1"},
            {company_id: 2, orders_numbers: "order 2"},
            {company_id: 1, orders_numbers: "order 1, order 3"},
            {company_id: 2, orders_numbers: "order 2, order 3"}
        ], "after insert three orders");


        // test update
        await db.query(`
            update orders set
                client_companies_ids = array[2,1]
            where id = 3
        `);
        result = await db.query(`
            select id, orders_numbers
            from companies
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, orders_numbers: "order 1, order 3"},
            {id: 2, orders_numbers: "order 2, order 3"}
        ], "after update last order");

        result = await db.query(`
            select *
            from company_updates
        `);
        assert.deepStrictEqual(result.rows, [
            {company_id: 1, orders_numbers: "order 1"},
            {company_id: 2, orders_numbers: "order 2"},
            {company_id: 1, orders_numbers: "order 1, order 3"},
            {company_id: 2, orders_numbers: "order 2, order 3"}
        ], "after insert three orders");
    });

});