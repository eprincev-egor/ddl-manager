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

    it("build cache when exists dependency to function", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache-with-func-and-cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table documents (
                id serial primary key,
                invoice_orders_ids bigint[],
                gtd_orders_ids bigint[]
            );
            
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
});