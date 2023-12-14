import assert, { strict } from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../../getDbClient";
import { DDLManager } from "../../../../lib/DDLManager";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { Pool } from "pg";

use(chaiShallowDeepEqualPlugin);

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/DDLManager.build cache", () => {
    let db: Pool;
    
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
        await db.end();
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

        assert.deepStrictEqual(result.rows[0], {
            orders_profit: "100"
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
            orders_profit: null,
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
            returning id, orders_profit;
        `);

        assert.deepStrictEqual(result.rows[0], {
            id: 1,
            orders_profit: null
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
            returning id, orders_count
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
        await db.query(`
            delete from company_updates
        `);

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

    it("array_agg(value order by value asc/desc nulls last/first)", async() => {
        const folderPath = ROOT_TMP_PATH + "/array_agg_order_by";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer,
                test_value smallint
            );

            insert into companies default values;
        `);
        
        fs.writeFileSync(folderPath + "/array_agg.sql", `
            cache totals for companies (
                select
                    array_agg(
                        orders.test_value
                            order by orders.test_value
                            asc nulls first
                    ) as orders_values_asc_nulls_first,

                    array_agg(
                        orders.test_value
                            order by orders.test_value
                            asc nulls last
                    ) as orders_values_asc_nulls_last,

                    array_agg(
                        orders.test_value
                            order by orders.test_value
                            desc nulls first
                    ) as orders_values_desc_nulls_first,

                    array_agg(
                        orders.test_value
                            order by orders.test_value
                            desc nulls last 
                    ) as orders_values_desc_nulls_last

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


        // test insert
        await db.query(`
            insert into orders (
                id_client,
                test_value
            ) 
            values 
                (1, 1),
                (1, 2),
                (1, null)
        `);
        result = await db.query(`
            select
                id,
                orders_values_asc_nulls_first,
                orders_values_asc_nulls_last,
                orders_values_desc_nulls_first,
                orders_values_desc_nulls_last
            from companies
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            orders_values_asc_nulls_first: [null, 1, 2],
            orders_values_asc_nulls_last: [1, 2, null],
            orders_values_desc_nulls_first: [null, 2, 1],
            orders_values_desc_nulls_last: [2, 1, null]
        }]);
    });

    it("build cache with custom aggregation", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            CREATE OR REPLACE FUNCTION array_union(a ANYARRAY, b ANYARRAY)
            RETURNS ANYARRAY AS
            $$
                SELECT array_agg(distinct x order by x)
                FROM unnest(a || b) as x
            $$ LANGUAGE SQL;

            CREATE AGGREGATE array_union_agg(ANYARRAY) (
                SFUNC = array_union,
                STYPE = ANYARRAY,
                INITCOND = '{}'
            );
        
              
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer,
                countries_ids integer[]
            );

            insert into companies default values;
        `);
        
        fs.writeFileSync(folderPath + "/totals.sql", `
            cache totals for companies (
                select
                    array_union_agg( orders.countries_ids ) as orders_countries_ids
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

        // create one date
        await db.query(`
            insert into orders (id_client, countries_ids)
            values (1, array[1, 2]);
        `);
        result = await db.query(`
            select orders_countries_ids
            from companies
        `);
        assert.deepStrictEqual(result.rows, [{
            orders_countries_ids: [1, 2]
        }]);

        // add more
        await db.query(`
            insert into orders (id_client, countries_ids)
            values (1, array[2, 3]);
        `);
        result = await db.query(`
            select orders_countries_ids
            from companies
        `);
        assert.deepStrictEqual(result.rows, [{
            orders_countries_ids: [1, 2, 3]
        }]);
    });

    it("test cache universal triggers working", async() => {
        const folderPath = ROOT_TMP_PATH + "/universal_cache_test";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                order_number text,
                order_date date
            );
            create table order_company_link (
                id serial primary key,
                id_order integer,
                id_company integer
            );
        `);
        
        fs.writeFileSync(folderPath + "/orders.sql", `
            cache totals for companies (
                select
                    max( orders.order_date ) as max_order_date,
                    string_agg( distinct orders.order_number, ', ' ) as orders_numbers
                
                from order_company_link as link
                
                left join orders on
                    orders.id = link.id_order
                
                where
                    link.id_company = companies.id
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
            insert into companies default values;

            insert into orders (
                order_date,
                order_number
            ) 
            values 
                ('2020-12-26'::date, 'order-26'),
                ('2020-12-27'::date, 'order-27');

            insert into order_company_link (
                id_order, id_company
            )
            values 
                (1, 1),
                (2, 1);
        `);
        result = await db.query(`
            select
                id,
                max_order_date,
                orders_numbers
            from companies
        `);

        const date27 = new Date(2020, 11, 27);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            max_order_date: date27,
            orders_numbers: "order-26, order-27"
        }]);
    });


    it("build cache with self update", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key,
                a integer,
                b integer
            );

            insert into companies (a, b) values (10, 25);
        `);

        fs.writeFileSync(folderPath + "/self_update.sql", `
            cache totals for companies (
                select
                    companies.a + companies.b as c
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result;

        
        await db.query(`
            insert into companies (a, b)
            values (5, 100);
        `);
        result = await db.query(`
            select id, c
            from companies
            order by id
        `);
        expect(result.rows).to.be.shallowDeepEqual([
            {id: 1, c: 35},
            {id: 2, c: 105}
        ]);


        await db.query(`
            update companies set
                b = 26
            where id = 1;
        `);
        result = await db.query(`
            select id, c
            from companies
            order by id
        `);
        expect(result.rows).to.be.shallowDeepEqual([
            {id: 1, c: 36},
            {id: 2, c: 105}
        ]);
    });


    it("build two cache, with the second dependent on the first (self update and commutative)", async() => {
        const folderPath = ROOT_TMP_PATH + "/two-caches-with-self-row";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table tasks (
                id serial primary key,
                id_order integer
            );
            create table users (
                id serial primary key
            );
            create table user_task_watcher (
                id serial primary key,
                id_watcher integer not null,
                id_task integer not null
            );
            create table orders (
                id serial primary key,
                id_manager integer not null
            );

            insert into users default values;
            insert into users default values;
            insert into tasks (id_order) values (1);
            insert into user_task_watcher (id_task, id_watcher) values (1, 1);
            insert into orders (id_manager) values (2);
        `);
        
        fs.writeFileSync(folderPath + "/watchers.sql", `
            cache watchers for tasks (
                select
                    array_agg( link.id_watcher ) as watchers_ids
                from user_task_watcher as link
                where
                    link.id_task = tasks.id
            )
        `);
        fs.writeFileSync(folderPath + "/order_manager.sql", `
            cache order_manager for tasks (
                select
                    array_agg( orders.id_manager ) as orders_managers_ids
                from orders
                where
                    orders.id = tasks.id_order
            )
        `);
        fs.writeFileSync(folderPath + "/managers_and_watchers.sql", `
            cache managers_and_watchers for tasks (
                select
                    tasks.watchers_ids ||
                    tasks.orders_managers_ids as watchers_or_managers
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select
                watchers_ids,
                orders_managers_ids,
                watchers_or_managers
            from tasks
            where id = 1
        `);

        assert.deepStrictEqual(result.rows[0], {
            watchers_ids: [1],
            orders_managers_ids: [2],
            watchers_or_managers: [1, 2]
        });
    });

    it("build cache with index", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache-with-index";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key,
                orders_profit numeric default 0,
                event jsonb,
                name text
            );

            CREATE INDEX companies_fresh_rows_idx
              ON public.companies
              USING btree
              (id DESC NULLS LAST);

            CREATE INDEX companies_id_desc_idx
                ON public.companies
                USING btree
                (orders_profit, id nulls first);
            
            CREATE INDEX companies_lower_name_idx
                ON public.companies
                USING btree
                (lower(name));
            
            CREATE INDEX companies_event_idx
                ON public.companies
                USING gin
                (event jsonb_path_ops);
            
            CREATE INDEX companies_lower_name_sp2_idx
                ON public.companies
                USING btree
                (lower(name) varchar_pattern_ops);
            
            
            CREATE INDEX companies_lower_name_sp3_idx
                ON public.companies
                USING btree
                (lower(name) collate "POSIX" varchar_pattern_ops);
            
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
                    max( orders.id ) as last_order_id
                from orders
                where
                    orders.id_client = companies.id
            )
            index btree on (last_order_id)
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        const result = await db.query(`
            SELECT
                pg_get_indexdef(i.oid) AS indexdef,
                obj_description(i.oid) as comment
            FROM pg_index x
                JOIN pg_class c ON c.oid = x.indrelid
                JOIN pg_class i ON i.oid = x.indexrelid
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace
            WHERE
                (c.relkind = ANY(ARRAY['r'::"char", 'm'::"char"])) AND
                i.relkind = 'i'::"char" and
                
                c.relname = 'companies'
        `);
        const row = result.rows.find((someRow: {comment?: string}) => 
            someRow.comment &&
            someRow.comment.includes("ddl-manager")
        );

        assert.ok(
            /using\s+btree\s+\(\s*last_order_id\s*\)/i.test(row.indexdef),
            "created valid index"
        );
    });

    it("build functions/triggers before build cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache-with-func-and-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key,
                orders_profit numeric default 0
            );
            create table orders (
                id serial primary key,
                id_client integer,
                note text
            );

            insert into companies default values;
            insert into orders (id_client, note)
            values (1, 'test');
        `);
        
        fs.writeFileSync(folderPath + "/prepare_note.sql", `
            create or replace function prepare_note(note text)
            returns text as $body$
            begin
                return note || ': prepared';
            end
            $body$
            language plpgsql;
        `);
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            cache totals for companies (
                select
                    string_agg(
                        prepare_note( orders.note ),
                        ', '
                    ) as orders_notes
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
            select orders_notes
            from companies
            where id = 1
        `);
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            orders_notes: "test: prepared"
        });
    });

    it("don't update twice, if column has long name", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache-with-long-column-name";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer,
                this_is_some_source_column_name_with_order_note text
            );

            insert into companies default values;
            insert into orders (
                id_client, 
                this_is_some_source_column_name_with_order_note
            )
            values (1, 'test');
        `);
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            cache totals for companies (
                select
                    string_agg(
                        distinct
                            orders.this_is_some_source_column_name_with_order_note,
                        ', '
                    ) as string_agg_this_is_some_source_column_name_with_order_note
                from orders
                where
                    orders.id_client = companies.id
            )
        `);
        
        // first update
        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });
        const result = await db.query(`
            select string_agg_this_is_some_source_column_name_with_order_note
            from companies
            where id = 1
        `);
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            string_agg_this_is_some_source_column_name_with_order_note: "test"
        });

        // second update
        await db.query(`
            create or replace function stop_update_companies()
            returns trigger as $body$
            begin
                raise exception 'twice update error';
            end
            $body$
            language plpgsql;

            create trigger stop_update_companies
            after insert or update or delete
            on companies
            for each row
            execute procedure stop_update_companies();
        `);
        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

    });

    it("rebuild cache column if exists trigger dependency", async() => {
        const folderPath = ROOT_TMP_PATH + "/rebuild-column-with-cache-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key,
                orders_profit bigint
            );
            create table orders (
                id serial primary key,
                id_client integer,
                profit numeric
            );
        `);
        
        fs.writeFileSync(folderPath + "/dep_trigger.sql", `
            create or replace function test()
            returns trigger as $body$
            begin
                return new;
            end
            $body$
            language plpgsql;

            create trigger test
            after update of orders_profit
            on companies
            for each row 
            execute procedure test();
        `);
        fs.writeFileSync(folderPath + "/profit.sql", `
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

        fs.writeFileSync(folderPath + "/profit.sql", `
            cache totals for companies (
                select
                    string_agg( orders.profit::text, ',' ) as orders_profit
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
    });

    it("build one row cache trigger", async() => {
        const folderPath = ROOT_TMP_PATH + "/one-row-trigger";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table invoice (
                id serial primary key,
                id_list_contracts bigint
            );
            create table list_contracts (
                id serial primary key,
                date_contract date,
                contract_number text
            );
        `);
        
        fs.writeFileSync(folderPath + "/contract.sql", `
            cache one_row_contract for invoice (
                select
                    contract.date_contract as date_contract,
                    contract.contract_number as contract_number

                from list_contracts as contract
                where
                    contract.id = invoice.id_list_contracts
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        
        await db.query(`
            insert into list_contracts (contract_number) 
            values ('hello');

            insert into invoice (id_list_contracts)
            values (1);
        `);

        let result = await db.query(`
            select contract_number
            from invoice
        `);
        assert.deepStrictEqual(result.rows[0], {
            contract_number: "hello"
        });


        await db.query(`
            update list_contracts set
                contract_number = 'world'
        `);
        result = await db.query(`
            select contract_number
            from invoice
        `);
        assert.deepStrictEqual(result.rows[0], {
            contract_number: "world"
        });
    });

    it("build one row with join cache trigger", async() => {
        const folderPath = ROOT_TMP_PATH + "/one-row-trigger";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table invoice (
                id serial primary key,
                id_list_contracts bigint
            );
            create table list_contracts (
                id serial primary key,
                date_contract date,
                id_country bigint,
                contract_number text
            );
            create table countries (
                id serial primary key,
                name text
            );
        `);
        
        fs.writeFileSync(folderPath + "/contract.sql", `
            cache one_row_contract for invoice (
                select
                    contract.date_contract as date_contract,
                    contract.contract_number as contract_number,
                    coalesce(countries.name, 'RUS') as country_name

                from list_contracts as contract

                left join countries on
                    countries.id = contract.id_country

                where
                    contract.id = invoice.id_list_contracts
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        
        await db.query(`
            insert into countries (name) values ('ENG');

            insert into list_contracts (contract_number, id_country) 
            values ('hello', 1);

            insert into invoice (id_list_contracts)
            values (1);
        `);

        let result = await db.query(`
            select contract_number, country_name
            from invoice
        `);
        assert.deepStrictEqual(result.rows[0], {
            contract_number: "hello",
            country_name: "ENG"
        });


        await db.query(`
            update list_contracts set
                id_country = null
        `);
        result = await db.query(`
            select country_name
            from invoice
        `);
        assert.deepStrictEqual(result.rows[0], {
            country_name: "RUS"
        });
    });

    it("build cache when exists dependency to function", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache-with-func-and-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table documents (
                id serial primary key,

                -- need recreate column
                orders_ids text,

                invoice_orders_ids bigint[],
                gtd_orders_ids bigint[]
            );

            comment on column documents.orders_ids is $$
            ddl-manager-sync
            ddl-manager-select(select ',1,' as orders_ids)
            ddl-manager-cache(cache orders_ids for list_documents as doc)
            $$;

            insert into documents (invoice_orders_ids, gtd_orders_ids)
            values (
                array[1, null, 2]::bigint[],
                array[2, null, 3]::bigint[]
            );
        `);
        
        fs.writeFileSync(folderPath + "/distinct_array_without_nulls.sql", `
            create or replace function distinct_array_without_nulls(
                input_arr_ids bigint[]
            )
            returns bigint[] as $body$
            begin
                return (
                    select array_agg( distinct some_id )
                    from unnest( input_arr_ids ) as some_id
                    where
                        some_id is not null
                );
            end
            $body$
            language plpgsql;
        `);
        fs.writeFileSync(folderPath + "/arr_concat.sql", `
            cache arr_concat for documents (
                select
                    distinct_array_without_nulls(
                        documents.invoice_orders_ids ||
                        documents.gtd_orders_ids
                    ) as orders_ids
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select orders_ids
            from documents
        `);
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            orders_ids: [1,2,3]
        });
    });

    it("rebuild cache when exists dependency to other cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/rebuild-two-caches";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table user_task (
                id serial primary key,
                id_order bigint,
                department_users_ids bigint[]
            );
            create table public.order (
                id serial primary key,
                id_user_operations bigint
            );
        `);
        
        fs.writeFileSync(folderPath + "/order.sql", `
            cache order for user_task (
                select
                    array[orders.id_user_operations]::bigint[] as order_user_operations_ids
            
                from public.order as orders
                where
                    orders.id = user_task.id_order
            )
            without insert case on public.order
        `);
        fs.writeFileSync(folderPath + "/managers.sql", `
            cache managers for user_task (
                select
                    (user_task.department_users_ids ||
                    user_task.order_user_operations_ids) as managers_ids_and_owner
            )
            index gin on (managers_ids_and_owner)
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        // rebuild
        fs.writeFileSync(folderPath + "/order.sql", `
            cache order for user_task (
                select
                    orders.id_user_operations as order_user_operations_id
            
                from public.order as orders
                where
                    orders.id = user_task.id_order
            )
            without insert case on public.order
        `);
        fs.writeFileSync(folderPath + "/managers.sql", `
            cache managers for user_task (
                select
                    (user_task.department_users_ids ||
                    user_task.order_user_operations_id) as managers_ids_and_owner
            )
            index gin on (managers_ids_and_owner)
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

    });

    it("rebuild cache when exists dependency to other cache inside Where", async() => {
        const folderPath = ROOT_TMP_PATH + "/comment-target";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table user_task (
                id serial primary key,
                query_name text,
                row_id bigint,
                deleted smallint
            );
            create table comments (
                id serial primary key,
                query_name text,
                row_id bigint
            );
            create table list_gtd (
                id serial primary key,
                orders_ids bigint[],
                deleted smallint
            );
            create table operations (
                id serial primary key,
                id_order bigint,
                deleted smallint
            );
            create table orders (
                id serial primary key,
                id_company_crm bigint
            );
        `);
        
        fs.writeFileSync(folderPath + "/gtd.sql", `
            cache gtd for comments (
                select
                    gtd.orders_ids[1] as gtd_order_id
            
                from list_gtd as gtd
                where
                    gtd.id = comments.target_row_id and
                    gtd.deleted = 0 and
            
                    array_length( gtd.orders_ids, 1 ) = 1 and
            
                    comments.target_query_name in (
                        'LIST_ALL_GTD',
                        'LIST_ARCHIVE_GTD',
                        'LIST_ACTIVE_GTD',
                        'LIST_GTD'
                    )
            )
            without insert case on list_gtd
        `);

        fs.writeFileSync(folderPath + "/operations.sql", `
            cache operations for comments (
                select
                    operations.id_order as operation_id_order
            
                from operations
                where
                    operations.id = comments.target_row_id and
                    operations.deleted = 0 and
                    comments.target_query_name in (
                        'OPERATION',
                        'OPERATION_SEA',
                        'OPERATION_AUTO',
                        'OPERATION_TRAIN',
                        'OPERATION_AIR',
                        'OPERATION_FORWARD',
                        'OPERATION_SUB_DOC'
                    )
            )
            without insert case on operations
        `);
        fs.writeFileSync(folderPath + "/user_task.sql", `
            cache user_task for comments (
                select
                    coalesce(
                        user_task.query_name,
                        comments.query_name
                    ) as target_query_name,
                    
                    coalesce(
                        user_task.row_id,
                        comments.row_id
                    ) as target_row_id
            
                from user_task
                where
                    user_task.id = comments.row_id and
                    user_task.deleted = 0 and
                    comments.query_name = 'USER_TASK'
            )
            without insert case on user_task
        `);
        fs.writeFileSync(folderPath + "/order_id.sql", `
            cache order_id for comments (
                select
                    coalesce(
                        comments.gtd_order_id,
            
                        (case
                            when comments.target_query_name in ('ORDER', 'ORDER_REQUEST')
                            then comments.target_row_id
                        end)
            
                    ) as order_id,
            
                    (case
                        when comments.target_query_name = 'OPERATION_UNIT'
                        then comments.target_row_id
                    end) as unit_id
            )
        `);
        fs.writeFileSync(folderPath + "/company_crm_id.sql", `
            cache company_crm_id for comments (
                select
                    orders.id_company_crm as company_crm_id
            
                from orders
                where
                    orders.id = comments.order_id
            )
            without insert case on orders
            without insert case on comments
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        fs.writeFileSync(folderPath + "/user_task.sql", `
            cache user_task for comments (
                select
                    user_task.query_name as user_task_query_name,
                    user_task.row_id as user_task_row_id
            
                from user_task
                where
                    user_task.id = comments.row_id and
                    user_task.deleted = 0 and
                    comments.query_name = 'USER_TASK'
            )
            without insert case on user_task
        `);
        fs.writeFileSync(folderPath + "/target.sql", `
            cache target for comments (
                select
                    coalesce(
                        comments.user_task_query_name,
                        comments.query_name
                    ) as target_query_name,
            
                    coalesce(
                        comments.user_task_row_id,
                        comments.row_id
                    ) as target_row_id
            )
        `);
        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

    });

    it("one last row cache dependent on self update cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/last_comment_message";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table operation_unit (
                id serial primary key,
                name text
            );

            create table comments (
                id serial primary key,
                query_name text default 'OPERATION_UNIT',
                row_id bigint,
                message text
            );

            insert into operation_unit
                (name)
            values
                ('unit 1'),
                ('unit 2');
            
            insert into comments
                (row_id, message)
            values
                (1, 'comment X'),
                (1, 'comment Y'),
                (2, 'comment A'),
                (2, 'comment B'),
                (2, 'comment C');
        `);

        fs.writeFileSync(folderPath + "/a_unit_id.sql", `
            cache unit_id for comments (
                select
                    case
                        when comments.query_name = 'OPERATION_UNIT'
                        then comments.row_id
                    end as unit_id
            )
        `);

        fs.writeFileSync(folderPath + "/last_comment.sql", `
            cache last_comment for operation_unit (
                select
                    comments.message as last_comment
                from comments
                where
                    comments.unit_id = operation_unit.id
            
                order by comments.id desc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select id, last_comment
            from operation_unit
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, last_comment: "comment Y"},
            {id: 2, last_comment: "comment C"}
        ]);
    });

    it("cache first element of array and other dependent cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/array_element";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table comments (
                id serial primary key,
                query_name text,
                row_id bigint,
                message text
            );

            create table list_gtd (
                id serial primary key,
                deleted smallint
            );

            create table gtd_order_link (
                id serial primary key,
                id_order bigint,
                id_gtd bigint
            );

            create table orders (
                id serial primary key
            );

            insert into comments
                (query_name, row_id, message)
            values
                ('LIST_ALL_GTD', 1, 'gtd comment');
            
            insert into list_gtd
                (deleted)
            values
                (0);

            insert into orders default values;

            insert into gtd_order_link
                (id_order, id_gtd)
            values (1,1);
        `);

        fs.writeFileSync(folderPath + "/gtd_orders_ids.sql", `
            cache gtd_orders_ids for list_gtd (
                select
                    array_agg( link.id_order ) as orders_ids
            
                from gtd_order_link as link
                where
                    link.id_order = list_gtd.id
            )
        `);

        fs.writeFileSync(folderPath + "/gtd_order_id.sql", `
            cache gtd for comments (
                select
                    gtd.orders_ids[1] as gtd_order_id
            
                from list_gtd as gtd
                where
                    gtd.id = comments.row_id and
                    gtd.deleted = 0 and

                    array_length( gtd.orders_ids, 1 ) = 1 and
            
                    comments.query_name in (
                        'LIST_ALL_GTD',
                        'LIST_ARCHIVE_GTD',
                        'LIST_ACTIVE_GTD',
                        'LIST_GTD'
                    )
            )
        `);

        fs.writeFileSync(folderPath + "/order_id.sql", `
            cache order_id for comments (
                select
                    coalesce(
                        comments.gtd_order_id,
            
                        (case
                            when comments.query_name in ('ORDER', 'ORDER_REQUEST')
                            then comments.row_id
                        end)
            
                    ) as order_id
            )
        `);

        fs.writeFileSync(folderPath + "/last_comment.sql", `
            cache last_comment for orders (
                select
                    comments.message as last_comment
                from comments
                where
                    comments.order_id = orders.id
            
                order by comments.id desc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select id, last_comment
            from orders
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, last_comment: "gtd comment"}
        ]);
    });

    it("coalesce(parent_lvl, lvl) infinity recursion", async() => {
        const folderPath = ROOT_TMP_PATH + "/recursion-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table operations (
                id serial primary key,
                doc_number text,
                id_prev_operation bigint,
                id_doc_parent_operation bigint,
                deleted smallint default 0,
                units_ids bigint[]
            );

            create table units (
                id serial primary key
            );

            insert into units default values;
            insert into units default values;

            insert into operations
                (units_ids)
            values (array[1]);

            insert into operations
                (id_prev_operation, units_ids)
            values
                (1, array[2]);
        `);
        
        fs.writeFileSync(folderPath + "/parent.sql", `
            cache parent for operations as child_log_oper (
                select
                    parent_log_oper.lvl as parent_lvl
            
                from operations as parent_log_oper
                where
                    parent_log_oper.id = child_log_oper.id_prev_operation and
                    parent_log_oper.deleted = 0 and
                    parent_log_oper.id_doc_parent_operation is null and
                    child_log_oper.id_doc_parent_operation is null
            )
        `);
        
        fs.writeFileSync(folderPath + "/lvl.sql", `
            cache lvl for operations as log_oper (
                select
                    coalesce(
                        log_oper.parent_lvl + 1,
                        1
                    )::integer as lvl
            )
        `);
        
        fs.writeFileSync(folderPath + "/last_oper.sql", `
            cache last_oper for units (
                select
                    last_oper.doc_number as last_oper_doc_number

                from operations as last_oper
                where
                    last_oper.units_ids && array[ units.id ]::bigint[] and
                    last_oper.deleted = 0

                order by last_oper.lvl desc
                limit 1
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            insert into operations (id_prev_operation) values (2);
        `);

        const result = await db.query(`
            select id, parent_lvl, lvl
            from operations
            order by id
        `);

        assert.deepStrictEqual(result.rows, [
            {id: 1, parent_lvl: null, lvl: 1},
            {id: 2, parent_lvl: 1, lvl: 2},
            {id: 3, parent_lvl: 2, lvl: 3}
        ])
    });

    it("correct update cache rows with infinity recursion", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table operations (
                id serial primary key,
                id_prev_operation bigint
            );

            do $$
            declare new_id bigint;
            declare prev_id bigint;
            declare i bigint;
            declare j bigint;
            begin
                for i in (select index from generate_series(1, 100) as index) loop
                    prev_id = null;

                    for j in (select index from generate_series(1, 10) as index) loop
                        insert into operations (
                            id_prev_operation
                        ) values (
                            prev_id
                        )
                        returning id
                        into new_id;

                        prev_id = new_id;
                    end loop;

                end loop;
            end
            $$;

            create index operations_prev_indx
            on operations
            using btree
            (id_prev_operation);
        `);
        
        fs.writeFileSync(folderPath + "/parent.sql", `
            cache parent for operations as child_log_oper (
                select
                    parent_log_oper.lvl as parent_lvl
            
                from operations as parent_log_oper
                where
                    parent_log_oper.id = child_log_oper.id_prev_operation
            )
        `);
        
        fs.writeFileSync(folderPath + "/lvl.sql", `
            cache lvl for operations as log_oper (
                select
                    coalesce(
                        log_oper.parent_lvl + 1,
                        1
                    )::integer as lvl
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            with recursive
                all_operations as (
                    select
                        root_operation.id,
                        1 as expected_lvl,
                        root_operation.lvl as actual_lvl

                    from operations as root_operation
                    where
                        root_operation.id_prev_operation is null

                    union

                    select
                        next_operation.id,
                        prev_operation.expected_lvl + 1 as expected_lvl,
                        next_operation.lvl as actual_lvl
                    from
                        operations as next_operation,
                        all_operations as prev_operation
                    where
                        prev_operation.id = next_operation.id_prev_operation
                )
            select
                exists(
                    select from all_operations
                    where
                        expected_lvl is distinct from actual_lvl
                ) as has_wrong_lvl
        `);

        assert.strictEqual(
            result.rows[0].has_wrong_lvl,
            false,
            "exists wrong lvl"
        );
    });

    it("one-row with 'coalesce' trigger dependent on other one-row trigger", async() => {
        const folderPath = ROOT_TMP_PATH + "/recursion-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );

            create table operations (
                id serial primary key,
                id_order bigint,
                is_border_crossing smallint default 0 not null,
                deleted smallint default 0 not null,
                id_arrival_point_end integer
            );

            create table arrival_points (
                id serial primary key,
                id_operation bigint,
                id_point bigint,
                actual_departure_date timestamp without time zone,
                actual_date timestamp without time zone,
                expected_date timestamp without time zone,
                id_arrival_point_type smallint
            );
            
            insert into orders default values;
            insert into operations 
                (id_order, is_border_crossing, id_arrival_point_end)
            values
                (1, 1, 1);
            insert into arrival_points
                (id_operation, id_arrival_point_type)
            values
                (1, 2);
        `);
        
        fs.writeFileSync(folderPath + "/border_crossing.sql", `
            cache border_crossing for orders (
                select
                    border_crossing.id as id_border_crossing,

                    coalesce(
                        border_crossing.end_expected_date,
                        orders.date_delivery
                    ) as date_delivery,

                    coalesce(
                        border_crossing.id_point_end,
                        orders.id_point_delivery
                    ) as id_point_delivery
            
                from operations as border_crossing
                where
                    border_crossing.id_order = orders.id and
                    border_crossing.is_border_crossing = 1 and
                    border_crossing.deleted = 0
            
                order by border_crossing.id desc
                limit 1
            )
        `);
        
        fs.writeFileSync(folderPath + "/end_arr_point.sql", `
            cache end_arr_point for operations as oper (
                select
                    end_arr_point.id_point as id_point_end,
                    end_arr_point.actual_departure_date as end_actual_departure_date,
                    end_arr_point.actual_date as end_actual_date,
                    end_arr_point.expected_date as end_expected_date
            
                from arrival_points as end_arr_point
                where
                    end_arr_point.id = oper.id_arrival_point_end and
                    end_arr_point.id_arrival_point_type = 2
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            update arrival_points set
                id_point = 4
        `);

        const result = await db.query(`
            select id, id_point_delivery
            from orders
        `);

        assert.deepStrictEqual(result.rows[0], {
            id: 1,
            id_point_delivery: "4"
        });
    });

    it("multi agg inside hard expression", async() => {
        const folderPath = ROOT_TMP_PATH + "/strange-transit-cache";
        fs.mkdirSync(folderPath);

        await db.query(`

            create table units (
                id serial primary key,
                operations bigint[]
            );
            
            create table arrival_points (
                id serial primary key,
                id_operation bigint,
                actual_departure_date timestamp without time zone,
                actual_date timestamp without time zone,
                expected_date timestamp without time zone
            );
            
            insert into units (operations)
            values (array[1]), (array[2]), (array[1, 2]);

            insert into arrival_points (id_operation)
            values (1), (2);
        `);
        
        fs.writeFileSync(folderPath + "/transit.sql", `
            cache transit for units (
                select 
                    floor(
                        extract(
                            epoch from(
                                coalesce(
                                    max(ap.actual_date),
                                    max(ap.expected_date)
                                ) 
                                - min(ap.actual_departure_date)
                            )
                        ) 
                        / 60
                    ) as transit_period_minute
                from arrival_points as ap
                where units.operations && array[ ap.id_operation  ]::bigint[]
            );
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            update arrival_points set
                actual_date = now(),
                actual_departure_date = now()
            where id_operation = 1;

            update arrival_points set
                actual_date = now() - interval '1 minute',
                actual_departure_date = now() - interval '1 minute'
            where id_operation = 2;
        `);

        const result = await db.query(`
            select
                id,
                transit_period_minute::integer as transit_period_minute
            from units
            order by id
        `);

        assert.deepStrictEqual(result.rows, [
            {id: 1, transit_period_minute: 0},
            {id: 2, transit_period_minute: 0},
            {id: 3, transit_period_minute: 1}
        ]);
    });

    it("case/when need wrap to brackets inside IF ... THEN", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table units (
                id serial primary key,
                has_exped boolean,
                has_our_service_in_forward_operation smallint,
                deleted smallint default 0
            );
            create table operations (
                id serial primary key,
                id_unit bigint
            );

            insert into units (
                has_exped, has_our_service_in_forward_operation
            ) values (
                true, 1
            );
            insert into operations (id_unit)
            values (1);
        `);
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            cache totals for operations (
                select
                    (
                        CASE
                            WHEN units.has_exped IS TRUE AND units.has_our_service_in_forward_operation = 1 THEN 'Наша услуга'
                            WHEN units.has_exped IS TRUE THEN 'Да'
                            ELSE 'Нет'
                        END
                    ) as forward_for_auto
                from units
                where
                    units.id = operations.id_unit and
                    units.deleted = 0
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            insert into units (
                has_exped, has_our_service_in_forward_operation
            ) values (
                true, 0
            );
            insert into operations (id_unit)
            values (2);
        `);

        const result = await db.query(`
            select id, forward_for_auto
            from operations
            order by id
        `);

        assert.deepStrictEqual(result.rows, [
            {id: 1, forward_for_auto: "Наша услуга"},
            {id: 2, forward_for_auto: "Да"}
        ]);
    });

    it("cache sum() using existent column without cache rows", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table units (
                id serial primary key,
                accepted_weight numeric
            );
            create table accepted (
                id serial primary key,
                id_unit bigint,
                weight numeric
            );

            insert into units default values;
        `);
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            cache accepted for units (
                select
                    sum( accepted.weight ) as accepted_weight
                from accepted
                where
                    accepted.id_unit = units.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result = await db.query(`
            select id, accepted_weight
            from units
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, accepted_weight: null}
        ]);



        await db.query(`
            insert into accepted (
                id_unit, weight
            ) values (
                1, 400
            );
        `);
        result = await db.query(`
            select id, accepted_weight
            from units
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, accepted_weight: "400"}
        ]);
    });

    it("one last row by arr ref and some column", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache-arr-ref";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table operations (
                id serial primary key,
                id_operation_type smallint,
                id_doc_parent_operation bigint,
                deleted smallint default 0,
                units_ids bigint[]
            );

            create table units (
                id serial primary key,
                id_last_auto integer
            );
        `);

        fs.writeFileSync(folderPath + "/arr_concat.sql", `
            cache last_auto_doc for units (
                select
                    last_auto_doc.id as id_last_auto_doc
            
                from operations as last_auto_doc
                where
                    last_auto_doc.id_doc_parent_operation = units.id_last_auto
                    -- auto
                    and last_auto_doc.id_operation_type = 1
                    and last_auto_doc.deleted = 0
                    and last_auto_doc.units_ids && array[units.id]::bigint[]
            
                order by
                    last_auto_doc.id desc
                limit 1
            );
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            insert into operations
                (id_operation_type, deleted)
            values
                (1, 0);
            insert into operations
                (id_operation_type, deleted)
            values
                (1, 0);

            insert into units
                (id_last_auto)
            values
                (1),
                (2);
            
            insert into operations
                (id_operation_type, id_doc_parent_operation, deleted, units_ids)
            values
                (1, 1, 0, array[1, 2]),
                (1, 2, 0, array[1, 2]);
        `);

        const result = await db.query(`
            select id, id_last_auto_doc
            from units
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, id_last_auto_doc: 3},
            {id: 2, id_last_auto_doc: 4}
        ]);
    });

    it("self row cache with deps to func", async() => {
        const folderPath = ROOT_TMP_PATH + "/self-cache-get-curs";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table payment_orders_list_gtd_link (
                id serial primary key,
                gtd_id_currency integer,
                swift_id_currency integer,
                custom_curs double precision,
                conversion_date timestamp without time zone,
                gtd_date timestamp without time zone
            );
        `);

        fs.writeFileSync(folderPath + "/conversion_rate.sql", `
            cache conversion_rate for payment_orders_list_gtd_link (
                select
                    public.get_curs(
                        coalesce(payment_orders_list_gtd_link.gtd_id_currency, get_default_currency(), 1):: bigint,
                        coalesce(payment_orders_list_gtd_link.swift_id_currency, get_default_currency(), 1):: bigint,
                        payment_orders_list_gtd_link.custom_curs::double precision,
                        coalesce(payment_orders_list_gtd_link.conversion_date, payment_orders_list_gtd_link.gtd_date, now())::timestamp without time zone,
                        0::smallint,
                        payment_orders_list_gtd_link.id::bigint,
                        'PAYMENT_ORDERS_LIST_GTD_LINK'::text
                    ) as conversion_rate
            )
        `);
        fs.writeFileSync(folderPath + "/get_curs.sql", `
            CREATE OR REPLACE FUNCTION get_curs(
                currency_from_id BIGINT,
                currency_to_id BIGINT,
                custom_curs DOUBLE PRECISION,
                date_curs_to TIMESTAMP WITHOUT TIME ZONE,
                is_euro_zone_curs SMALLINT DEFAULT 0,
                reference_id BIGINT DEFAULT NULL,
                reference_table text DEFAULT ''
            )
                RETURNS NUMERIC
                LANGUAGE plpgsql
            AS
            $body$
            begin
                return custom_curs;
            end
            $body$;
        `);
        fs.writeFileSync(folderPath + "/get_default_currency.sql", `
            create or replace function public.get_default_currency() 
            returns bigint 
            language plpgsql
            as $body$
            BEGIN
                return 1;
            END;
            $body$
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            insert into payment_orders_list_gtd_link
                (custom_curs)
            values
                (66.6);
        `);

        const result = await db.query(`
            select conversion_rate
            from payment_orders_list_gtd_link
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {conversion_rate: "66.6"}
        ]);
    });

    it("array_union_agg", async() => {
        const folderPath = ROOT_TMP_PATH + "/array-union";
        fs.mkdirSync(folderPath);

        await db.query(`
            CREATE FUNCTION array_union_all(a ANYARRAY, b ANYARRAY)
            RETURNS ANYARRAY AS
            $$
            SELECT array_agg(x)
            FROM (
                SELECT unnest(a) x
                UNION ALL
                SELECT unnest(b)
            ) AS u
            $$ LANGUAGE SQL;

            CREATE AGGREGATE array_union_all_agg(ANYARRAY) (
            SFUNC = array_union_all,
            STYPE = ANYARRAY,
            INITCOND = '{}'
            );

            CREATE FUNCTION array_union(a ANYARRAY, b ANYARRAY)
            RETURNS ANYARRAY AS
            $$
            SELECT array_agg(distinct x)
            FROM unnest(a || b) as x
            $$ LANGUAGE SQL;

            CREATE AGGREGATE array_union_agg(ANYARRAY) (
            SFUNC = array_union,
            STYPE = ANYARRAY,
            INITCOND = '{}'
            );

            create table orders (
                id serial primary key,
                number text,
                id_invoice integer
            );
            create table invoices (
                id serial primary key,
                id_payment integer
            );
            create table payments (
                id serial primary key
            );

            insert into orders (number, id_invoice) 
            values
                ('order-A', 1),
                ('order-B', 1),
                ('order-C', 2),
                ('order-D', 2),
                ('order-E', 3),
                ('order-F', 3);
            
            insert into invoices (id_payment)
            values
                (1), (1), (2);

            insert into payments default values;
            insert into payments default values;
        `);

        fs.writeFileSync(folderPath + "/invoices_orders.sql", `
            cache orders for invoices (
                select
                    array_agg(distinct orders.number) as orders_numbers
                from orders
                where
                    orders.id_invoice = invoices.id
            )
        `);
        fs.writeFileSync(folderPath + "/payment_orders.sql", `
            cache orders for payments (
                select
                    array_union_agg(invoices.orders_numbers) as orders_numbers
                from invoices
                where
                    invoices.id_payment = payments.id
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result = await db.query(`
            select payments.id, payments.orders_numbers
            from payments
            order by payments.id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, orders_numbers: ["order-A", "order-B", "order-C", "order-D"]},
            {id: 2, orders_numbers: ["order-E", "order-F"]}
        ]);

        await db.query(`
            update invoices set
                id_payment = 2
            where id = 2;
        `);
        result = await db.query(`
            select payments.id, payments.orders_numbers
            from payments
            order by payments.id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, orders_numbers: ["order-A", "order-B"]},
            {id: 2, orders_numbers: ["order-C", "order-D", "order-E", "order-F"]}
        ]);


        await db.query(`
            update orders set
                number = 'order-X'
            where number in ('order-B', 'order-C', 'order-D');
        `);
        result = await db.query(`
            select payments.id, payments.orders_numbers
            from payments
            order by payments.id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, orders_numbers: ["order-A", "order-X"]},
            {id: 2, orders_numbers: ["order-E", "order-F", "order-X"]}
        ]);
    });


    it("array_union_all_agg", async() => {
        const folderPath = ROOT_TMP_PATH + "/array-union-all";
        fs.mkdirSync(folderPath);

        await db.query(`
            CREATE FUNCTION array_union_all(a ANYARRAY, b ANYARRAY)
            RETURNS ANYARRAY AS
            $$
            SELECT array_agg(x)
            FROM (
                SELECT unnest(a) x
                UNION ALL
                SELECT unnest(b)
            ) AS u
            $$ LANGUAGE SQL;

            CREATE AGGREGATE array_union_all_agg(ANYARRAY) (
            SFUNC = array_union_all,
            STYPE = ANYARRAY,
            INITCOND = '{}'
            );

            create table invoices (
                id serial primary key,
                id_payment integer,
                orders_numbers text[]
            );
            create table payments (
                id serial primary key
            );

            insert into invoices (id_payment, orders_numbers)
            values
                (1, ARRAY['X', 'Y']),
                (1, ARRAY['Y', 'Z']),
                (2, ARRAY['Z', 'A', 'B']);

            insert into payments default values;
            insert into payments default values;
        `);

        fs.writeFileSync(folderPath + "/payment_orders.sql", `
            cache orders for payments (
                select
                    array_union_all_agg(invoices.orders_numbers) as orders_numbers
                from invoices
                where
                    invoices.id_payment = payments.id
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result = await db.query(`
            select payments.id, payments.orders_numbers
            from payments
            order by payments.id
        `);
        assert.deepStrictEqual( sort(result.rows), [
            {id: 1, orders_numbers: ["X", "Y", "Y", "Z"]},
            {id: 2, orders_numbers: ["A", "B", "Z"]}
        ]);

        await db.query(`
            update invoices set
                id_payment = 2
            where id = 2;
        `);
        result = await db.query(`
            select payments.id, payments.orders_numbers
            from payments
            order by payments.id
        `);
        assert.deepStrictEqual( sort(result.rows), [
            {id: 1, orders_numbers: ["X", "Y"]},
            {id: 2, orders_numbers: ["A", "B", "Y", "Z", "Z"]}
        ]);


        await db.query(`
            update invoices set
                orders_numbers = ARRAY['A', 'B']
            where id = 3;
        `);
        result = await db.query(`
            select payments.id, payments.orders_numbers
            from payments
            order by payments.id
        `);
        assert.deepStrictEqual( sort(result.rows), [
            {id: 1, orders_numbers: ["X", "Y"]},
            {id: 2, orders_numbers: ["A", "B", "Y", "Z"]}
        ]);
    
        function sort(rows: any[]) {
            for(const row of rows) {
                row.orders_numbers.sort();
            }
            return rows;
        }
    });

    it("array_union_agg, filter by array", async() => {
        const folderPath = ROOT_TMP_PATH + "/array-union";
        fs.mkdirSync(folderPath);

        await db.query(`
            CREATE FUNCTION array_union_all(a ANYARRAY, b ANYARRAY)
            RETURNS ANYARRAY AS
            $$
            SELECT array_agg(x)
            FROM (
                SELECT unnest(a) x
                UNION ALL
                SELECT unnest(b)
            ) AS u
            $$ LANGUAGE SQL;

            CREATE AGGREGATE array_union_all_agg(ANYARRAY) (
            SFUNC = array_union_all,
            STYPE = ANYARRAY,
            INITCOND = '{}'
            );

            CREATE FUNCTION array_union(a ANYARRAY, b ANYARRAY)
            RETURNS ANYARRAY AS
            $$
            SELECT array_agg(distinct x)
            FROM unnest(a || b) as x
            $$ LANGUAGE SQL;

            CREATE AGGREGATE array_union_agg(ANYARRAY) (
            SFUNC = array_union,
            STYPE = ANYARRAY,
            INITCOND = '{}'
            );

            create table invoices (
                id serial primary key,
                payments_ids integer[],
                units_numbers text[]
            );
            create table payments (
                id serial primary key
            );

            insert into invoices (payments_ids, units_numbers)
            values
                (ARRAY[1, 2], ARRAY['A', 'B', 'C']), 
                (ARRAY[1], ARRAY['A', 'E']), 
                (ARRAY[2], ARRAY['B', 'D']);

            insert into payments default values;
            insert into payments default values;
        `);

        fs.writeFileSync(folderPath + "/payment_orders.sql", `
            cache orders for payments (
                select
                    array_union_agg(invoices.units_numbers) as units_numbers
                from invoices
                where
                    invoices.payments_ids && ARRAY[payments.id]::integer[]
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result = await db.query(`
            select payments.id, payments.units_numbers
            from payments
            order by payments.id
        `);
        assert.deepStrictEqual( sort(result.rows), [
            {id: 1, units_numbers: ["A", "B", "C", "E"]},
            {id: 2, units_numbers: ["A", "B", "C", "D"]}
        ]);


        await db.query(`
            update invoices set
                payments_ids = ARRAY[3]
            where id = 1;
        `);
        result = await db.query(`
            select payments.id, payments.units_numbers
            from payments
            order by payments.id
        `);
        assert.deepStrictEqual( sort(result.rows), [
            {id: 1, units_numbers: ["A", "E"]},
            {id: 2, units_numbers: ["B", "D"]}
        ]);


        await db.query(`
            update invoices set
                units_numbers = ARRAY['X', 'Y']
            where id in (2, 3);
        `);
        result = await db.query(`
            select payments.id, payments.units_numbers
            from payments
            order by payments.id
        `);
        assert.deepStrictEqual( sort(result.rows), [
            {id: 1, units_numbers: ["X", "Y"]},
            {id: 2, units_numbers: ["X", "Y"]}
        ]);

        function sort(rows: any[]) {
            for(const row of rows) {
                row.units_numbers.sort();
            }
            return rows;
        }
    });

    it("rebuild cache and check deps", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer,
                profit numeric(14, 2),
                deleted smallint
            );

            insert into companies default values;
            insert into orders 
                (id_client, profit, deleted)
            values 
                (1, 200, 0),
                (1, 800, 1);
        `);
        fs.writeFileSync(folderPath + "/source.sql", `
            cache orders for companies (
                select
                    sum( orders.profit ) as total_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `);
        fs.writeFileSync(folderPath + "/deps1.sql", `
            cache client for orders (
                select
                    100 * orders.profit / client.total_profit as percent_of_client_profit
                from companies as client
                where
                    client.id = orders.id_client
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        fs.writeFileSync(folderPath + "/source.sql", `
            cache orders for companies (
                select
                    sum( orders.profit ) as total_profit
                from orders
                where
                    orders.id_client = companies.id and
                    orders.deleted = 0
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select
                id, 
                percent_of_client_profit::integer as percent_of_client_profit
            from orders
            order by id
        `);

        expect(result.rows).to.be.shallowDeepEqual([
            {id: 1, percent_of_client_profit: 100},
            {id: 2, percent_of_client_profit: 400}
        ]);
    });

    it("build cache when exists trigger on table and exists two same name tables inside different schemas", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            drop schema if exists operation cascade;
            create schema operation;

            create table public.companies (
                id serial primary key
            );
            create table operation.companies (
                id serial primary key
            );
            
            insert into public.companies default values;
            insert into operation.companies default values;
        `);

        fs.writeFileSync(folderPath + "/public.cache.sql", `
            cache public1 for public.companies as companies (
                select
                    companies.id + 1 as id1
            )
        `);
        fs.writeFileSync(folderPath + "/public.trigger.sql", `
            create or replace function test_public()
            returns trigger as $body$
            begin
                --raise exception 'need disable me (public)';
                return new;
            end;
            $body$ language plpgsql;

            create trigger test_public
            after update
            on public.companies
            for each row
            execute procedure test_public();
        `);

        fs.writeFileSync(folderPath + "/operation.cache.sql", `
            cache operation2 for operation.companies as companies (
                select
                    companies.id + 2 as id2
            )
        `);
        fs.writeFileSync(folderPath + "/operation.trigger.sql", `
            create or replace function test_operation()
            returns trigger as $body$
            begin
                --raise exception 'need disable me (operation)';
                return new;
            end;
            $body$ language plpgsql;

            create trigger test_operation
            after update
            on operation.companies
            for each row
            execute procedure test_operation();
        `);
        

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select 
                public.companies.id as public_id, 
                public.companies.id1 as public_id1,

                operation.companies.id as operation_id, 
                operation.companies.id2 as operation_id2

            from public.companies, operation.companies
            limit 1
        `);

        expect(result.rows).to.be.shallowDeepEqual([{
            public_id: 1,
            public_id1: 2,
            operation_id: 1,
            operation_id2: 3
        }]);
    });

    it("remove some trigger and update cache on table", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table public.companies (
                id serial primary key
            );
            insert into public.companies default values;
        `);

        const someTriggerPath = folderPath + "/public.trigger.sql";

        fs.writeFileSync(folderPath + "/public.trigger.sql", `
            create or replace function test_public()
            returns trigger as $body$
            begin
                --raise exception 'need disable me (public)';
                return new;
            end;
            $body$ language plpgsql;

            create trigger test_public
            after update
            on public.companies
            for each row
            execute procedure test_public();
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        fs.unlinkSync(someTriggerPath);

        fs.writeFileSync(folderPath + "/public.cache.sql", `
            cache public1 for public.companies as companies (
                select
                    companies.id + 1 as id1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select 
                public.companies.id as public_id, 
                public.companies.id1 as public_id1
            from public.companies, operation.companies
        `);

        expect(result.rows).to.be.shallowDeepEqual([{
            public_id: 1,
            public_id1: 2
        }]);
    });

    it("two root columns with same name, but different tables", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table invoices (
                id serial primary key,
                type text not null,
                id_payment integer,
                id_order integer,
                sum integer
            );
            create table payments (
                id serial primary key,
                doc_number text
            );
            create table orders (
                id serial primary key,
                doc_number text
            );

            insert into orders (doc_number) values ('order 1');
            insert into payments (doc_number) values ('payment 1');
            insert into invoices 
                (type, id_payment, id_order, sum)
            values
                ('incoming', 1, 1, 1000);
        `);
        fs.writeFileSync(folderPath + "/order_invoices.sql", `
            cache order_invoices for orders (
                select
                    array_agg(invoices.id) as invoices_ids
                from invoices
                where
                    invoices.id_order = orders.id
            )
            index gin on (invoices_ids)
        `);
        fs.writeFileSync(folderPath + "/payment_invoices.sql", `
            cache payment_invoices for payments (
                select
                    array_agg(invoices.id) as invoices_ids
                from invoices
                where
                    invoices.id_payment = payments.id
            )
            index gin on (invoices_ids)
        `);
        fs.writeFileSync(folderPath + "/invoice_payments.sql", `
            cache payments for invoices (
                select
                    string_agg( 
                        distinct payments.doc_number, ', ' 
                        order by payments.doc_number
                    ) as payments_numbers
                from payments
                where
                    payments.invoices_ids && array[ invoices.id ]
            )
        `);
        fs.writeFileSync(folderPath + "/invoice_orders.sql", `
            cache orders for invoices (
                select
                    string_agg( 
                        distinct orders.doc_number, ', ' 
                        order by orders.doc_number
                    ) as orders_numbers
                from orders
                where
                    orders.invoices_ids && array[ invoices.id ]
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select
                id, 
                payments_numbers,
                orders_numbers
            from invoices
            order by id
        `);

        expect(result.rows).to.be.shallowDeepEqual([
            {id: 1, payments_numbers: "payment 1", orders_numbers: "order 1"}
        ]);
    });

    it("rebuild cache in correct order, when exists ABC A->B, B->C, A->C", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        const profit = 10000;

        await db.query(`
            create table orders (
                id serial primary key,
                profit integer
            );
            insert into orders (profit) values (10000);
        `);
        // C dependent on A
        // C dependent on B
        // B dependent on A

        fs.writeFileSync(folderPath + "/c.sql", `
            cache c for orders (
                select
                    orders.a + orders.b + 1000 as c
            )
        `);
        fs.writeFileSync(folderPath + "/b.sql", `
            cache b for orders (
                select
                    orders.a + 10 as b
            )
        `);
        fs.writeFileSync(folderPath + "/a.sql", `
            cache a for orders (
                select
                    orders.profit + 1 as a
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select
                id, profit,
                a, b, c
            from orders
            order by id
        `);

        const a = profit + 1;
        const b = a + 10;
        const c = a + b + 1000;

        expect(result.rows).to.be.shallowDeepEqual([{
            id: 1,
            profit,
            a, b, c
        }]);
    });

    it("column used in two other columns with same name", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                debit integer,
                credit integer
            );
            create table payments (
                id serial primary key,
                id_order integer
            );
            create table invoices (
                id serial primary key,
                id_order integer
            );

            insert into orders (debit, credit) 
            values (1000, 600);

            insert into payments (id_order)
            values (1);
            insert into invoices (id_order)
            values (1);
        `);

        fs.writeFileSync(folderPath + "/profit.sql", `
            cache profit for orders (
                select
                    orders.debit - orders.credit as profit
            )
        `);
        fs.writeFileSync(folderPath + "/payment_order.sql", `
            cache payment_order for payments (
                select
                    orders.profit as order_profit
                from orders
                where orders.id = payments.id_order
            )
        `);
        fs.writeFileSync(folderPath + "/invoice_order.sql", `
            cache invoice_order for invoices (
                select
                    orders.profit as order_profit
                from orders
                where orders.id = invoices.id_order
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result = await db.query(`
            select
                orders.profit as order,
                invoices.order_profit as invoice,
                payments.order_profit as payment
            from orders, invoices, payments
            limit 1
        `);

        expect(result.rows).to.be.shallowDeepEqual([{
            order: 400,
            invoice: 400,
            payment: 400
        }]);



        await db.query(`
            update orders set
                credit = debit
        `);

        result = await db.query(`
            select
                orders.profit as order,
                invoices.order_profit as invoice,
                payments.order_profit as payment
            from orders, invoices, payments
            limit 1
        `);
        expect(result.rows).to.be.shallowDeepEqual([{
            order: 0,
            invoice: 0,
            payment: 0
        }]);
    });

    it("case/when with bool_and inside", async() => {

        await db.query(`
            create table public.order (
                id serial primary key
            );
            create table invoice (
                id serial primary key,
                orders_ids bigint[],
                id_invoice_type integer,
                deleted smallint default 0,
                payment_date timestamp without time zone
            );

            DROP FUNCTION IF EXISTS last_agg(anyelement, anyelement) cascade;
            CREATE FUNCTION last_agg(anyelement, anyelement) RETURNS anyelement
                LANGUAGE sql IMMUTABLE STRICT
                AS $_$
                    SELECT $2;
            $_$;
            CREATE AGGREGATE last(anyelement) (
                SFUNC = last_agg,
                STYPE = anyelement
            );
            
            insert into public.order default values;

            insert into invoice (
                orders_ids, 
                payment_date, 
                id_invoice_type
            )
            values (array[1], now(), 2);
        `);

        let folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        fs.writeFileSync(folderPath + "/invoice_payment_data.sql", `
            cache invoice_payment_data 
            for public.order (

                select
                    case 
                        when
                            bool_and(invoice.payment_date is not null)
                        then last(invoice.id order by invoice.payment_date)
                        else null
                    end 
                    as id_last_outgoing_invoice
                from invoice
                where
                    invoice.id_invoice_type in (2,3) and
                    invoice.orders_ids && array[ public.order.id ]::bigint[] and
                    invoice.deleted = 0
            )
        `);

        await DDLManager.build({
            db, 
            folder: [
                folderPath
            ]
        });


        await db.query(`
            update invoice set
                payment_date = now() + interval '1 day';    
        `);
        const result = await db.query(`
            select id_last_outgoing_invoice
            from public.order
        `);
        assert.deepEqual(result.rows[0], {
            id_last_outgoing_invoice: 1
        });
    });

    it("case/when with count(id) and max() inside", async() => {

        await db.query(`
            create table unit (
                id serial primary key
            );

            create table supply_order_position_unit_link (
                id serial primary key,
                is_apportionment_parent smallint default 0,
                id_unit integer,
                position_deleted smallint default 0
            );

            insert into unit default values;

            insert into supply_order_position_unit_link
                (id_unit, is_apportionment_parent)
            values (1, 1)
        `);

        let folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        fs.writeFileSync(folderPath + "/invoice_payment_data.sql", `
            cache link_totals for unit (
                select
                    (
                        case 
                            when 
                                count(link.id) = 1 and max(link.is_apportionment_parent) = 1
                            then 1
                            else 0
                        end
                    ) as is_single_apportionment_parent
            
                from supply_order_position_unit_link as link
                where
                    link.id_unit = unit.id and
                    link.position_deleted = 0
            )
        `);

        await DDLManager.build({
            db, 
            folder: [
                folderPath
            ]
        });


        await db.query(`
            update supply_order_position_unit_link set
                is_apportionment_parent = 0
        `);
        const result = await db.query(`
            select is_single_apportionment_parent
            from unit
        `);
        assert.deepEqual(result.rows[0], {
            is_single_apportionment_parent: 0
        });
    });

    it("not null on cache column", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table companies (
                id serial primary key,
                orders_profit numeric default 0 not null
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
                    coalesce(sum( orders.profit ), 0) as orders_profit
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
            insert into companies default values
            returning orders_profit
        `);
        assert.deepStrictEqual(result.rows[0], {
            orders_profit: "0"
        });
    });

    it("case when x then null else integer", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table my_table (
                id serial primary key,
                quantity integer
            );
        `);
        
        fs.writeFileSync(folderPath + "/has_10_quantity.sql", `
            cache totals for my_table (
                select
                    (case
                    when my_table.quantity < 10
                    then null
                    else 1
                    end) as has_10_quantity
            )
        `);
        await db.query(`
            insert into my_table (quantity)
            values (5), (10)
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select id, quantity, has_10_quantity
            from my_table
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, quantity: 5, has_10_quantity: null},
            {id: 2, quantity: 10, has_10_quantity: 1}
        ]);
    });

    it("case when x then 0 else 1::numeric", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table my_table (
                id serial primary key,
                quantity integer
            );
        `);
        
        fs.writeFileSync(folderPath + "/has_10_quantity.sql", `
            cache totals for my_table (
                select
                    (case
                    when my_table.quantity < 10
                    then 0
                    else my_table.quantity::float / 10
                    end) as has_10_quantity
            )
        `);
        await db.query(`
            insert into my_table (quantity)
            values (5), (13)
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select id, quantity, has_10_quantity
            from my_table
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, quantity: 5, has_10_quantity: 0},
            {id: 2, quantity: 13, has_10_quantity: 1.3}
        ]);
    });


    it("don't drop column if it's legacy column", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table my_table (
                id serial primary key,
                quantity integer
            );
        `);
        
        fs.writeFileSync(folderPath + "/cache.sql", `
            cache totals for my_table (
                select
                    1 as quantity
            )
        `);
        await db.query(`
            insert into my_table default values;
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result = await db.query(`
            select id, quantity
            from my_table
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, quantity: 1,}
        ]);


        // kill cache
        fs.unlinkSync(folderPath + "/cache.sql");

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        result = await db.query(`
            select id, quantity
            from my_table
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, quantity: 1,}
        ]);

    });

    it("don't update column twice", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table updates_counter (
                count integer
            );
            insert into updates_counter (count) values (0);

            create table my_table (
                id serial primary key,
                quantity integer
            );
            insert into my_table default values;
        `);
        fs.writeFileSync(folderPath + "/counter.sql", `
            create or replace function test()
            returns trigger as $body$
            begin
                update updates_counter set
                    count = count + 1;

                return new;
            end
            $body$ language plpgsql;

            create trigger test
            before update
            on my_table
            for each row execute procedure test()
        `);
        fs.writeFileSync(folderPath + "/cache.sql", `
            cache totals for my_table (
                select
                    random() as quantity
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });
        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query(`
            select count 
            from updates_counter
        `);
        strict.deepEqual(result.rows[0], {count: 1});
    });

    it("need change column type even if column used in custom trigger", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                value integer,
                some_column text,
                changes_counter integer default 0
            );
            insert into orders (value) values (1);

            
            create function test()
            returns trigger as $body$
            begin
                if new.some_column is distinct from old.some_column then
                    new.changes_counter = new.changes_counter + 1;
                end if;

                return new;
            end
            $body$
            language plpgsql;

            create trigger test
            before update of some_column, value
            on orders
            for each row
            execute procedure test();
        `);
        fs.writeFileSync(folderPath + "/some_column.sql", `
            cache totals for orders (
                select
                    orders.value * 2 as some_column
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        // check that old trigger works
        await db.query(`
            update orders set
                value = 2;
        `);
        const result = await db.query(`
            select id, some_column, changes_counter
            from orders
        `);
        strict.deepEqual(result.rows, [
            {id: 1, some_column: 4, changes_counter: 2}
        ]);
    });

    it("using old array column (bigint[] => integer[])", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                invoices_ids bigint[],
                order_number text
            );
            create table invoices (
                id serial primary key,
                orders_ids bigint[],
                doc_number text
            );
            create table invoice_order_link (
                id serial primary key,
                id_order integer,
                id_invoice integer
            );

            insert into orders (order_number, invoices_ids)
            values
                ('ORDER-1', ARRAY[1]::bigint[]),
                ('ORDER-2', ARRAY[2, 3]::bigint[]);

            insert into invoices (doc_number, orders_ids)
            values
                ('INVOICE-1', ARRAY[1]::bigint[]),
                ('INVOICE-2', ARRAY[2]::bigint[]),
                ('INVOICE-3', ARRAY[2]::bigint[]);

            insert into invoice_order_link (id_order, id_invoice)
            values (1, 1), (2, 2), (2, 3);
        `);
        fs.writeFileSync(folderPath + "/invoices_ids.sql", `
            cache invoices_ids for orders (
                select
                    array_agg(link.id_invoice) as invoices_ids
                from invoice_order_link as link
                where
                    link.id_order = orders.id
            )
        `);
        fs.writeFileSync(folderPath + "/invoice_numbers.sql", `
            cache invoice_numbers for orders (
                select
                    string_agg(
                        distinct invoices.doc_number,
                        ', '
                        order by invoices.doc_number
                    ) as invoices_numbers
                from invoices
                where
                    invoices.id = any( orders.invoices_ids )
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            update invoices set
                doc_number = 'UPDATED-' || doc_number;
        `);
        const result = await db.query(`
            select id, invoices_numbers
            from orders
            order by id
        `);
        strict.deepEqual(result.rows, [
            {id: 1, invoices_numbers: "UPDATED-INVOICE-1"},
            {id: 2, invoices_numbers: "UPDATED-INVOICE-2, UPDATED-INVOICE-3"}
        ]);
    });

    it("using old array column (integer[] => bigint[])", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                invoices_ids integer[],
                order_number text
            );
            create table invoices (
                id serial primary key,
                orders_ids integer[],
                doc_number text
            );
            create table invoice_order_link (
                id serial primary key,
                id_order bigint,
                id_invoice bigint
            );

            insert into orders (order_number, invoices_ids)
            values
                ('ORDER-1', ARRAY[1]::bigint[]),
                ('ORDER-2', ARRAY[2, 3]::bigint[]);

            insert into invoices (doc_number, orders_ids)
            values
                ('INVOICE-1', ARRAY[1]::bigint[]),
                ('INVOICE-2', ARRAY[2]::bigint[]),
                ('INVOICE-3', ARRAY[2]::bigint[]);

            insert into invoice_order_link (id_order, id_invoice)
            values (1, 1), (2, 2), (2, 3);
        `);
        fs.writeFileSync(folderPath + "/invoices_ids.sql", `
            cache invoices_ids for orders (
                select
                    array_agg(link.id_invoice) as invoices_ids
                from invoice_order_link as link
                where
                    link.id_order = orders.id
            )
        `);
        fs.writeFileSync(folderPath + "/invoice_numbers.sql", `
            cache invoice_numbers for orders (
                select
                    string_agg(
                        distinct invoices.doc_number,
                        ', '
                        order by invoices.doc_number
                    ) as invoices_numbers
                from invoices
                where
                    invoices.id = any( orders.invoices_ids )
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            update invoices set
                doc_number = 'UPDATED-' || doc_number;
        `);
        const result = await db.query(`
            select id, invoices_numbers
            from orders
            order by id
        `);
        strict.deepEqual(result.rows, [
            {id: 1, invoices_numbers: "UPDATED-INVOICE-1"},
            {id: 2, invoices_numbers: "UPDATED-INVOICE-2, UPDATED-INVOICE-3"}
        ]);
    });
    
    it("stop build ddl on failed parsing", async() => {
        fs.writeFileSync(ROOT_TMP_PATH + "/invoice_numbers.sql", `
            cache invoice_numbers for orders (
                select
                    1
            )
        `);

        await strict.rejects(DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        }), /required alias/);
    });

    it("change column types from text to math formulas", async() => {
        await db.query(`
            create table orders (
                id serial primary key,
                column_a text,
                column_b text
            );
            insert into orders default values;
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
            cache totals for orders (
                select
                    orders.id * 2 as column_a,
                    orders.column_a * 3 as column_b
            )
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });

        const result = await db.query(`
            select id, column_a, column_b
            from orders
            order by id
        `);
        strict.deepEqual(result.rows, [
            {id: 1, column_a: 2, column_b: 6}
        ]);
    });

    it("twice build column with recursion deps in order by/limit", async() => {
        await db.query(`
            create table orders (
                id serial primary key
            );
            create table operations (
                id serial primary key,
                id_order integer,
                id_parent_oper integer,
                sale integer,
                buy integer
            );
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/parent_lvl.sql", `
            cache parent_lvl for operations (
                select
                    parent_oper.lvl as parent_lvl
                from operations as parent_oper
                where parent_oper.id = operations.id_parent_oper
            )
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/lvl.sql", `
            cache lvl for operations (
                select
                    coalesce(
                        operations.parent_lvl + 1,
                        1
                    )::integer as lvl
            )
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/first_oper.sql", `
            cache first_oper for orders (
                select
                    operations.sale as first_sale,
                    operations.buy as first_buy

                from operations
                where
                    operations.id_order = orders.id

                order by operations.lvl asc
                limit 1
            )
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/profit.sql", `
            cache profit for orders (
                select
                    coalesce(orders.first_sale, 0) - coalesce(orders.first_buy, 0)
                        as profit
            )
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/self.sql", `
            cache self for orders (
                select
                    orders.profit / orders.first_sale as x,
                    orders.profit / orders.first_buy as y,
                    orders.x + orders.y as z
            )
        `);

        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });

        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });


        await db.query(`
            insert into orders default values;

            insert into operations (
                id_order,
                id_parent_oper,
                buy, sale
            )
            values
                (1, null, 100, 200),
                (1, 1, 300, 400);
        `);
        const result = await db.query(`
            select id, z
            from orders
        `);
        strict.deepEqual(result.rows, [
            {id: 1, z: 1}
        ]);
    });

    it("correct build cache with make_interval", async() => {
        await db.query(`
            create table orders (
                id serial primary key,
                date_start timestamp without time zone,
                expected_days integer
            );
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/self.sql", `
            cache self for orders (
                select
                    orders.date_start + make_interval(days => 
                        orders.expected_days
                    ) as expected_date_end
            )
        `);

        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });


        await db.query(`
            insert into orders (date_start, expected_days)
            values (now(), 10);
        `);
        const result = await db.query(`
            select 
                orders.expected_date_end = orders.date_start + make_interval(days => 
                    orders.expected_days
                ) as is_valid_cache_value
            from orders
        `);
        strict.deepEqual(result.rows, [
            {is_valid_cache_value: true}
        ]);
    });

    it("cache with && operator in where and mutable cache row dep", async() => {
        await db.query(`
            create table orders (
                id serial primary key,
                nomenclature text
            );
            create table invoices (
                id serial primary key,
                orders_ids bigint[],
                nomenclature text,
                sum numeric
            );
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/invoices.sql", `
            cache invoices for orders (
                select sum(invoices.sum) as invoices_sum
                from invoices
                where
                    invoices.orders_ids && ARRAY[ orders.id ] and
                    invoices.nomenclature = orders.nomenclature
            )
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });


        await db.query(`
            insert into invoices
                (orders_ids, nomenclature, sum)
            values
                (array[1], 'red', 100),
                (array[1, 2], 'red', 200),
                (array[1], 'green', 400);
        `);
        await db.query(`
            insert into orders (nomenclature)
            values ('red');
        `);

        const result = await db.query(`
            select 
                invoices_sum
            from orders
        `);
        strict.deepEqual(result.rows, [
            {invoices_sum: "300"}
        ]);
    });

    it("two tables references to third by array", async() => {
        await db.query(`
            create table orders (
                id serial primary key,
                units_ids integer[]
            );
            create table invoices (
                id serial primary key,
                units_ids bigint[],
                sum numeric
            );
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/invoices.sql", `
            cache invoices for orders (
                select sum(invoices.sum) as invoices_sum
                from invoices
                where
                    invoices.units_ids && orders.units_ids
            )
        `);

        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });


        await db.query(`
            insert into invoices
                (units_ids, sum)
            values
                (array[1], 100),
                (array[1, 2], 200),
                (array[1], 400);
        `);
        await db.query(`
            insert into orders (units_ids)
            values (array[2]);
        `);
        const result = await db.query(`
            select 
                invoices_sum
            from orders
        `);
        strict.deepEqual(result.rows, [
            {invoices_sum: "200"}
        ]);
    });

    it("search by text[] column", async() => {
        await db.query(`
            create table companies (
                id serial primary key,
                name text,
                role text[],
                terminals_ids integer[]
            );
            create table terminals (
                id serial primary key
            );
            
            insert into terminals default values;
            insert into companies (name, role, terminals_ids)
            values ('test', array['client'], array[1]);
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
            cache clients for terminals (
                select
                    string_agg(distinct client.name, ', ') as clients
                from companies as client
                where
                    client.terminals_ids && array[terminals.id] and
                    client.role && array['client']::text[]
            )
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });


        await db.query(`
            update companies set
                name = 'UPDATED-' || companies.name;
        `);
        const result = await db.query(`
            select id, clients
            from terminals
        `);
        strict.deepEqual(result.rows, [
            {id: 1, clients: "UPDATED-test"}
        ]);
    });

    it("four levels of triggers, rebuilding custom triggers when it dependent on cache", async() => {
        await db.query(`
            create table companies (
                id serial primary key,
                a text,
                b text,
                c text,
                d text,
                e text
            );
            insert into companies default values;
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/custom1.sql", `
            create or replace function custom1()
            returns trigger as $body$
            begin
                new.b = new.a || '-b';
                return new;
            end
            $body$ language plpgsql;

            create trigger a_custom1
            before insert or update of a
            on companies
            for each row
            execute procedure custom1();
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/cache1.sql", `
            cache cache_z for companies (
                select
                    companies.b || '-c' as c
            )
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/cache2.sql", `
            cache cache_a for companies (
                select
                    companies.c || '-d' as d
            )
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/custom2.sql", `
            create or replace function custom2()
            returns trigger as $body$
            begin
                new.e = new.d || '-e';
                return new;
            end
            $body$ language plpgsql;

            create trigger z_custom2
            before insert or update of d
            on companies
            for each row
            execute procedure custom2();
        `);

        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });

        const result = await db.query(`
            update companies set
                a = 'a'
            returning a, b, c, d, e
        `);
        strict.deepEqual(result.rows, [{
            a: "a",
            b: "a-b",
            c: "a-b-c",
            d: "a-b-c-d",
            e: "a-b-c-d-e"
        }]);
    });

    describe("using old column with not null and changing that column type", () => {

        it("integer => numeric", async() => {
            await db.query(`
                create table orders (
                    id serial primary key,
                    sales integer not null,
                    buys integer not null,
                    profit integer not null
                );
    
                insert into orders (sales, buys, profit)
                values (1000, 800, 200);
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/profit.sql", `
                cache profit for orders (
                    select
                        (orders.sales - orders.buys)::numeric(14, 2) as profit
                )
            `);
    
            await DDLManager.build({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
    
            strict.ok(true, "no error: column \"profit\" contains null values");
        });

        it("integer => text", async() => {
            await db.query(`
                create table orders (
                    id serial primary key,
                    some_column integer not null
                );
    
                insert into orders (some_column)
                values (123);
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache cache for orders (
                    select
                        orders.id::text as some_column
                )
            `);
    
            await DDLManager.build({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
    
            strict.ok(true, "no error: column \"some_column\" contains null values");
        });

        it("text => integer", async() => {
            await db.query(`
                create table orders (
                    id serial primary key,
                    some_column text not null
                );
    
                insert into orders (some_column)
                values ('123'), ('abc');
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache cache for orders (
                    select
                        orders.id::integer as some_column
                )
            `);
    
            await DDLManager.build({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
    
            strict.ok(true, "no error: invalid input syntax for integer: \"abc\"")
        });

    });

    it("need drop cache column if it was created by ddl-manager", async() => {
        await db.query(`
            create table companies (
                id serial primary key
            );
            insert into companies default values;
        `);
        
        fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
            cache self for companies (
                select
                    companies.id + 2 as id2
            )
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        fs.unlinkSync(ROOT_TMP_PATH + "/cache.sql");
        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });


        const result = await db.query("select * from companies");
        assert.deepStrictEqual(result.rows[0], {
            id: 1
        });
    });

    it("need drop cache column if it was created by ddl-manager older version", async() => {
        await db.query(`
            create table companies (
                id serial primary key,
                id2 integer
            );
            insert into companies default values;

            comment on column companies.id2 is 'ddl-manager-sync';
        `);
        
        fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
            cache self for companies (
                select
                    companies.id + 2 as id2
            )
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        fs.unlinkSync(ROOT_TMP_PATH + "/cache.sql");
        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });


        const result = await db.query("select * from companies");
        assert.deepStrictEqual(result.rows[0], {
            id: 1
        });
    });

    it("need return back old column type", async() => {
        await db.query(`
            create table companies (
                id serial primary key,
                some_column text
            );
        `);
        
        fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
            cache self for companies (
                select
                    companies.id + 2 as some_column
            )
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        fs.unlinkSync(ROOT_TMP_PATH + "/cache.sql");
        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });


        const result = await db.query(`
            insert into companies (some_column) 
            values ('test')
            returning id, some_column
        `);
        assert.deepStrictEqual(result.rows[0], {
            id: 1,
            some_column: "test"
        });
    });

    it("need return not null for old column", async() => {
        await db.query(`
            create table companies (
                id serial primary key,
                some_column text not null
            );
        `);
        
        fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
            cache self for companies (
                select
                    companies.id + 2 as some_column
            )
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        fs.unlinkSync(ROOT_TMP_PATH + "/cache.sql");
        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });

        await assert.rejects(db.query(`
            insert into companies default values
        `), /not-null constraint/);
    });

    it("column can contain nulls, try return not null constraint", async() => {
        await db.query(`
            create table companies (
                id serial primary key,
                x integer,
                some_column text not null
            );
        `);
        
        fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
            cache self for companies (
                select
                    companies.x + 2 as some_column
            )
        `);

        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        await db.query(`
            insert into companies (x) values (null);
        `);


        fs.unlinkSync(ROOT_TMP_PATH + "/cache.sql");
        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        strict.ok(true, "no errors");
    });

    it("enable cache triggers on build ddl", async() => {
        await db.query(`
            create table companies (
                id serial primary key
            );
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
            cache self for companies (
                select
                    companies.id * 2 as id2
            )
        `);

        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        await db.query(`
            alter table companies
                disable trigger all;
        `);
        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });

        const result = await db.query(`
            insert into companies default values
            returning id2
        `);
        assert.deepStrictEqual(result.rows, [{id2: 2}])
    });

    it("old custom trigger dependent on old column", async() => {
        await db.query(`
            create table companies (
                id serial primary key,
                name text,
                note text
            );
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/old_cache.sql", `
            cache self for companies (
                select
                    companies.id * 2 as id2
            )
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/old_custom_trigger.sql", `
            function old_custom_trigger()
            returns trigger as $body$
            begin
                new.note = 'hello';
                return new;
            end
            $body$ language plpgsql;

            create trigger old_custom_trigger
            before update of id2
            on companies
            for each row
            execute procedure old_custom_trigger();
        `);
        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });

        fs.unlinkSync(ROOT_TMP_PATH + "/old_cache.sql");
        fs.unlinkSync(ROOT_TMP_PATH + "/old_custom_trigger.sql");

        fs.writeFileSync(ROOT_TMP_PATH + "/new_cache.sql", `
            cache self2 for companies (
                select
                    companies.name || coalesce(companies.note, '--') as name_note
            )
        `);
        
        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });

        const result = await db.query(`
            insert into companies (name)
            values ('test')
            returning name_note
        `);
        assert.deepStrictEqual(result.rows, [{
            name_note: "test--"
        }])
    });

    it("changing column type and testing one-row-trigger", async() => {
        await db.query(`
            create table companies (
                id serial primary key,
                some_column text
            );
            create table orders (
                id serial primary key,
                id_client integer
            );
            insert into companies default values;
            insert into orders (id_client) values (1);
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
            cache totals for orders (
                select
                    coalesce(companies.some_column, companies.some_column) as some_column
                from companies
                where
                    companies.id = orders.id_client
            )
        `);
        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });

        await db.query(`
            drop trigger if exists cache_totals_for_orders_on_companies 
                on public.companies;

            alter table companies
                drop column some_column;

            alter table companies
                add column some_column integer;

            create trigger cache_totals_for_orders_on_companies 
            after insert or delete or update of some_column 
            on public.companies 
            for each row 
            execute procedure cache_totals_for_orders_on_companies();
        `);
        
        await db.query(`
            delete from companies
        `);
    });

    describe("buildNew/dropOld", () => {

        it("build new columns without deleting old", async() => {
            await db.query(`
                create table companies (
                    id serial primary key
                );
                insert into companies default values;
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache self for companies (
                    select
                        companies.id * 2 as id2
                )
            `);
            await DDLManager.build({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache self for companies (
                    select
                        companies.id * 3 as id3
                )
            `);
    
            await DDLManager.buildNew({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
    
            const result = await db.query(`
                select id2, id3
                from companies
            `);
            assert.deepStrictEqual(result.rows, [{id2: 2, id3: 3}])
        });
    
        it("buildNew: dont drop old function", async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/func.sql", `
                create or replace function old_func()
                returns text as $body$
                begin
                    return 'old';
                end
                $body$ language plpgsql;
            `);
            await DDLManager.build({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
            fs.unlinkSync(ROOT_TMP_PATH + "/func.sql");
    
            await DDLManager.buildNew({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
    
            const result = await db.query(`
                select old_func() as result
            `);
            assert.deepStrictEqual(result.rows, [{result: "old"}])
        });
    
        it("build columns types with unknown triggers on column", async() => {
            await db.query(`
                create table companies (
                    id serial primary key,
                    idx integer
                );
                insert into companies default values;

                create or replace function bad_unknown_trigger()
                returns trigger as $body$
                begin
                    return new;
                end
                $body$ language plpgsql;

                create trigger bad_unknown_trigger
                after update of idx
                on companies
                for each row
                execute procedure bad_unknown_trigger();

            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache self for companies (
                    select
                        companies.id * 2 as idx
                )
            `);
            await DDLManager.build({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache self for companies (
                    select
                        companies.id::text as idx
                )
            `);
    
            await DDLManager.buildNew({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
    
            const result = await db.query(`
                select idx
                from companies
            `);
            assert.deepStrictEqual(result.rows, [{idx: "1"}])
        });
    
        it("build columns types with unknown triggers on table with keyword name", async() => {
            await db.query(`
                create table public.order (
                    id serial primary key,
                    idx integer
                );
                insert into public.order default values;

                create or replace function bad_unknown_trigger()
                returns trigger as $body$
                begin
                    return new;
                end
                $body$ language plpgsql;

                create trigger bad_unknown_trigger
                after update of idx
                on public.order
                for each row
                execute procedure bad_unknown_trigger();

            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache self for public.order (
                    select
                    public.order.id * 2 as idx
                )
            `);
            await DDLManager.build({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache self for public.order (
                    select
                    public.order.id::text as idx
                )
            `);
    
            await DDLManager.buildNew({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
    
            const result = await db.query(`
                select idx
                from public.order
            `);
            assert.deepStrictEqual(result.rows, [{idx: "1"}])
        });
    
        it("dropOld: drop old function", async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/func.sql", `
                create or replace function old_func()
                returns text as $body$
                begin
                    return 'old';
                end
                $body$ language plpgsql;
            `);
            await DDLManager.build({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
            fs.unlinkSync(ROOT_TMP_PATH + "/func.sql");
    
            await DDLManager.dropOld({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
    
            await assert.rejects(db.query(`
                select old_func() as result
            `), /error: function old_func\(\) does not exist/);
        });
    
        it("delete old columns without building new", async() => {
            await db.query(`
                create table companies (
                    id serial primary key
                );
                insert into companies default values;
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache self for companies (
                    select
                        companies.id * 2 as id2
                )
            `);
            await DDLManager.build({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache self for companies (
                    select
                        companies.id * 3 as id3
                )
            `);
    
            await DDLManager.dropOld({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
    
            const result = await db.query(`
                select *
                from companies
            `);
            assert.deepStrictEqual(result.rows, [{id: 1}])
        });
    
        it("build new cache without dropping custom triggers", async() => {
            await db.query(`
                create table companies (
                    id serial primary key,
                    name text
                );
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache self for companies (
                    select
                        companies.id * 2 as id2
                )
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/custom_trigger.sql", `
                create or replace function custom_trigger()
                returns trigger as $body$
                begin
                    new.name = 'test';
                    return new;
                end;
                $body$ language plpgsql;

                create trigger custom_trigger
                before insert or update of name
                on companies
                for each row execute procedure
                custom_trigger();

            `);
            await DDLManager.buildNew({
                db, 
                folder: ROOT_TMP_PATH,
                throwError: true
            });
    
            const result = await db.query(`
                insert into companies default values
                returning id, id2, name
            `);
            assert.deepStrictEqual(result.rows, [{
                id: 1, id2: 2,
                name: "test"
            }])
        });

    });

    // TODO: views can use column (check change column type)
    // TODO: custom before update trigger can be without "update of" list of columns

});