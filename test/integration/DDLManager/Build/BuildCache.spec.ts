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
            select *
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

            create or replace function max_or_null_date(
                prev_date timestamp with time zone,
                next_date timestamp with time zone
            ) returns timestamp with time zone as $body$
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
                final_date timestamp with time zone
            ) returns timestamp with time zone as $body$
            begin
                if final_date = '0001-01-01 00:00:00' then
                    return null;
                end if;

                return final_date;
            end
            $body$
            language plpgsql;


            CREATE AGGREGATE max_or_null_date_agg (timestamp with time zone)
            (
                sfunc = max_or_null_date,
                finalfunc = max_or_null_date_final,
                stype = timestamp with time zone,
                initcond = '0001-01-01T00:00:00.000Z'
            );
              
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer,
                customs_date timestamp with time zone
            );

            insert into companies default values;
        `);
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            cache totals for companies (
                select
                    max_or_null_date_agg( orders.customs_date ) as orders_customs_date
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
        let row;
        const date = new Date();


        // create one date
        await db.query(`
            insert into orders (id_client, customs_date)
            values (1, '${date.toISOString()}'::timestamp with time zone);
        `);
        result = await db.query(`
            select orders_customs_date
            from companies
        `);
        row = result.rows[0];
        assert.strictEqual(
            row.orders_customs_date && row.orders_customs_date.toISOString(),
            date.toISOString()
        );

        // add null
        await db.query(`
            insert into orders (id_client, customs_date)
            values (1, null);
        `);
        result = await db.query(`
            select orders_customs_date
            from companies
        `);
        row = result.rows[0];
        assert.strictEqual(
            row.orders_customs_date,
            null
        );
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
            select *
            from companies
        `);

        const date26 = new Date(2020, 11, 26);
        const date27 = new Date(2020, 11, 27);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            max_order_date_order_date: [
                date26,
                date27
            ],
            max_order_date: date27,
            orders_numbers: "order-26, order-27",
            orders_numbers_order_number: [
                "order-26",
                "order-27"
            ]
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
            select *
            from tasks
            where id = 1
        `);
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            watchers_ids: [1],
            orders_managers_ids: [2],
            watchers_or_managers: [1,2]
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
                (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char"])) AND
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
            select id, transit_period_minute
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
                            WHEN units.has_exped IS TRUE AND has_our_service_in_forward_operation = 1 THEN 'Наша услуга'
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

    it("correct sort dependencies", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                profit integer
            );
        `);
        
        fs.writeFileSync(folderPath + "/a.sql", `
            cache a for orders (
                select
                    orders.profit + 1 as profit_a
            )
        `);
        fs.writeFileSync(folderPath + "/b.sql", `
            cache b for orders (
                select
                    orders.profit_c + 1000 + orders.profit_a as profit_b
            )
        `);
        fs.writeFileSync(folderPath + "/c.sql", `
            cache c for orders (
                select
                    orders.profit_a + 10 as profit_c
            )
        `);
        fs.writeFileSync(folderPath + "/d.sql", `
            cache d for orders (
                select
                    orders.profit_c + 100 + orders.profit_a as profit_d
            )
        `);
        fs.writeFileSync(folderPath + "/e.sql", `
            cache e for orders (
                select
                    orders.profit_b + 100 as profit_e
            )
        `);
        fs.writeFileSync(folderPath + "/f.sql", `
            cache f for orders (
                select
                    orders.profit_d + 100 as profit_f
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`insert into orders (profit) values (10000)`);

        const result = await db.query(`select * from orders`);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            profit: 10000,
            profit_a: 10001,
            profit_b: 21012,
            profit_c: 10011,
            profit_d: 20112,
            profit_e: 21112,
            profit_f: 20212
        }]);
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

});