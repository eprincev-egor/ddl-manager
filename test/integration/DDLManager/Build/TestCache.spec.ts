import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../../getDbClient";
import { DDLManager } from "../../../../lib/DDLManager";

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

    it("test cache with self update", async() => {
        const folderPath = ROOT_TMP_PATH + "/gtd_dates";
        fs.mkdirSync(folderPath);

        const someDate = "2021-02-20 10:10:10";

        await db.query(`
            create table list_gtd (
                id serial primary key,
                date_clear timestamp without time zone,
                date_conditional_clear timestamp without time zone,
                date_release_for_procuring timestamp without time zone
            );

            insert into list_gtd (
                date_release_for_procuring
            ) values (
                '${someDate}'
            );
        `);

        fs.writeFileSync(folderPath + "/gtd.sql", `
            cache self_dates for list_gtd (
                select
                    coalesce(
                        list_gtd.date_clear,
                        list_gtd.date_conditional_clear,
                        list_gtd.date_release_for_procuring
                    ) as clear_date_total
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        // check default values
        const result = await db.query(`
            select
                id,
                clear_date_total::text as clear_date_total

            from list_gtd
        `);

        assert.deepStrictEqual(result.rows, [{
            id: 1,
            clear_date_total: someDate
        }]);
    });

    it("test cache with custom agg", async() => {
        const folderPath = ROOT_TMP_PATH + "/max_or_null";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table public.order (
                id serial primary key
            );

            create table fin_operation (
                id serial primary key,
                id_order bigint,
                fin_type text,
                profit numeric,
                deleted smallint default 0
            );

            insert into public.order default values;
        `);

        fs.writeFileSync(folderPath + "/orders.sql", `
            cache fin_totals for public.order (
                select
                    sum( fin_operation.profit ) filter (where
                        fin_operation.fin_type = 'red'
                    ) as sum_red,
                    sum( fin_operation.profit ) filter (where
                        fin_operation.fin_type = 'green'
                    ) as sum_green

                from fin_operation
                where
                    fin_operation.id_order = public.order.id and
                    fin_operation.deleted = 0
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        // test create 3 operations
        await db.query(`
            insert into fin_operation (
                id_order, fin_type, profit
            ) 
            values 
                (1, null, 1000),
                (1, 'red', 100),
                (1, 'green', 10)
        `);
        await testOrder({
            id: 1,
            sum_red: "100",
            sum_green: "10"
        });


        // test set second fin_operation type to null
        await db.query(`
            update fin_operation set
                fin_type = null,
                profit = 2000
            where
                id = 2
        `);
        await testOrder({
            id: 1,
            sum_red: "0",
            sum_green: "10"
        });


        async function testOrder(expectedRow: {
            id: number,
            sum_red: string | null,
            sum_green: string | null
        }) {
            const result = await db.query(`
                select
                    id,
                    sum_red,
                    sum_green
                from public.order
            `);
            assert.deepStrictEqual(result.rows, [expectedRow]);
        }
    });

    it("test set deleted = 1, when not changed orders_ids", async() => {
        const folderPath = ROOT_TMP_PATH + "/deleted_and_orders_ids";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table public.order (
                id serial primary key
            );

            create table list_gtd (
                id serial primary key,
                gtd_number text,
                orders_ids bigint[],
                deleted smallint default 0
            );

            insert into public.order default values;
            insert into public.order default values;

            insert into list_gtd (
                gtd_number,
                deleted,
                orders_ids
            ) values (
                'gtd 1',
                0,
                array[1]
            );
            insert into list_gtd (
                gtd_number,
                deleted,
                orders_ids
            ) values (
                'gtd 2',
                0,
                array[1]
            )
        `);

        fs.writeFileSync(folderPath + "/gtd_totals.sql", `
            cache gtd_totals for public.order (
                select
                    string_agg(distinct gtd.gtd_number, ', ') as gtd_numbers

                from list_gtd as gtd
                where
                    gtd.orders_ids && ARRAY[public.order.id]::bigint[] and
                    gtd.deleted = 0
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        // set deleted = 1
        await db.query(`
            update list_gtd set
                deleted = 1
            where
                id = 1
        `);
        let result = await db.query(`
            select gtd_numbers
            from public.order
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {gtd_numbers: "gtd 2"},
            {gtd_numbers: null}
        ]);


        // set deleted = 0 and other orders_ids
        await db.query(`
            update list_gtd set
                deleted = 0,
                orders_ids = array[2]
            where
                id = 1
        `);
        result = await db.query(`
            select gtd_numbers
            from public.order
        `);
        assert.deepStrictEqual(result.rows, [
            {gtd_numbers: "gtd 2"},
            {gtd_numbers: "gtd 1"}
        ]);
    });

    it("last comment message", async() => {
        const folderPath = ROOT_TMP_PATH + "/last_comment_message";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table operation_unit (
                id serial primary key,
                name text
            );

            create table comments (
                id serial primary key,
                unit_id bigint,
                message text
            );

            insert into operation_unit
                (name)
            values
                ('unit 1'),
                ('unit 2');
            
            insert into comments
                (unit_id, message)
            values
                (1, 'comment X'),
                (1, 'comment Y'),
                (2, 'comment A'),
                (2, 'comment B'),
                (2, 'comment C');
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

        const testSql = `
create or replace function check_units(check_label text)
returns void as $body$
declare invalid_units text;
begin

    select
        string_agg(
            '    id: ' || operation_unit.id || E'\n' ||
            '    name: ' || coalesce(operation_unit.name, '<NULL>') || E'\n' ||
            '    should_be_comment: ' || coalesce(should_be.last_comment, '<NULL>') || E'\n' ||
            '    actual_comment: ' || coalesce(operation_unit.last_comment, '<NULL>')

            , E'\n\n'
        )
    into
        invalid_units

    from operation_unit

    left join lateral (
        select
            comments.message as last_comment
        from comments
        where
            comments.unit_id = operation_unit.id

        order by comments.id desc
        limit 1
    ) as should_be on true
    where
        should_be.last_comment is distinct from operation_unit.last_comment;


    if invalid_units is not null then
        raise exception E'\n%, invalid units:\n%\n\n\ncomments:\n%\n\n\n', 
            check_label,
            invalid_units,
            (
                select
                string_agg(
                    '    id: ' || comments.id || E',\n' ||
                    '    unit_id: ' || coalesce(comments.unit_id::text, '<NULL>') || E',\n' ||
                    '    comment: ' || coalesce(comments.message, '<NULL>') || E',\n' ||
                    '    __last_comment_for_operation_unit: ' || coalesce(comments.__last_comment_for_operation_unit::text, '<NULL>')

                    , E'\n\n'
                )
                from comments
            );
    else
        raise notice '% - success', check_label;
    end if;
end
$body$
language plpgsql;

do $$
begin
    PERFORM check_units('label OLD ROWS - 1');

    update comments set
        message = message || 'updated';
    PERFORM check_units('label OLD ROWS - 2');

    delete from comments;
    delete from operation_unit;
    ALTER SEQUENCE comments_id_seq RESTART WITH 1;
    ALTER SEQUENCE operation_unit_id_seq RESTART WITH 1;    

    insert into operation_unit (name) values ('unit 1');
    insert into operation_unit (name) values ('unit 2');

    insert into comments (message)
    values ('comment A');
    PERFORM check_units('label 1');

    update comments set
        unit_id = 1
    where id = 1;
    PERFORM check_units('label 2');

    update comments set
        unit_id = 2
    where id = 1;
    PERFORM check_units('label 3');

    update comments set
        unit_id = 3
    where id = 1;
    PERFORM check_units('label 4');

    update comments set
        unit_id = 1
    where id = 1;
    PERFORM check_units('label 5');

    insert into comments (message, unit_id)
    values ('comment B', 1);
    PERFORM check_units('label 6');

    update comments set
        message = 'comment B - updated'
    where id = 2;
    PERFORM check_units('label 7');

    update comments set
        message = 'comment A - updated'
    where id = 1;
    PERFORM check_units('label 8');

    update comments set
        unit_id = null
    where id = 2;
    PERFORM check_units('label 9');

    update comments set
        message = 'comment A - update 2'
    where id = 1;
    PERFORM check_units('label 10');

    update comments set
        message = 'comment B - update 2'
    where id = 2;
    PERFORM check_units('label 11');

    delete from comments;
    PERFORM check_units('label 12');


    insert into comments (unit_id, message)
    values
        (1, 'Comment A.X'),
        (1, 'Comment B.X'),
        (1, 'Comment C.X')
    ;
    PERFORM check_units('label 13');

    insert into comments (unit_id, message)
    values
        (2, 'Comment A.Y'),
        (2, 'Comment B.Y'),
        (2, 'Comment C.Y')
    ;
    PERFORM check_units('label 14');


    update comments set
        unit_id = 1,
        message = 'Comment B.Y - updated'
    where
        message = 'Comment B.Y';
    PERFORM check_units('label 15');


    update comments set
        unit_id = 2,
        message = 'Comment B.Y - updated 2'
    where
        message = 'Comment B.Y - updated';
    PERFORM check_units('label 16');


    update comments set
        unit_id = 1,
        message = 'Comment C.Y - updated'
    where
        message = 'Comment C.Y';
    PERFORM check_units('label 17');


    raise exception 'success';
end
$$;
        `;

        let actualErr: Error = new Error("expected error");
        try {
            await db.query(testSql);
        } catch(err) {
            actualErr = err;
        }

        assert.strictEqual(actualErr.message, "success");
    });

    it("first auto number", async() => {
        const folderPath = ROOT_TMP_PATH + "/first_auto_number";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table public.order (
                id serial primary key,
                name text
            );
            
            create table operations (
                id serial primary key,
                id_order bigint,
                type text,
                deleted smallint,
                doc_number text,
                incoming_date text
            );

            insert into public.order
                (name)
            values
                ('order 1'),
                ('order 2');
            
            insert into operations
                (id_order, type, deleted, doc_number)
            values
                (1, 'auto', 0, 'auto 1'),
                (1, 'sea',  0, 'sea 1'),
                (2, 'auto', 0, 'auto A'),
                (2, 'auto', 0, 'auto B'),
                (2, 'auto', 0, 'auto C');
        `);

        fs.writeFileSync(folderPath + "/first_auto.sql", `
            cache first_auto for public.order (
                select
                    operations.doc_number as first_auto_number,
                    operations.incoming_date as first_incoming_date
            
                from operations
                where
                    operations.id_order = public.order.id
                    and
                    operations.type = 'auto'
                    and
                    operations.deleted = 0
            
                order by operations.id asc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const testSql = `

        create or replace function check_orders(check_label text)
        returns void as $body$
        declare invalid_orders text;
        begin
        
            select
                string_agg(
                    '    id: ' || public.order.id || E'\n' ||
                    '    name: ' || coalesce(public.order.name, '<NULL>') || E'\n' ||
                    '    should_be: ' || coalesce(should_be.first_auto_number, '<NULL>') || E'\n' ||
                    '    actual: ' || coalesce(public.order.first_auto_number, '<NULL>')
        
                    , E'\n\n'
                )
            into
                invalid_orders
        
            from public.order
        
            left join lateral (
                select
                    operations.doc_number as first_auto_number
        
                from operations
                where
                    operations.id_order = public.order.id
                    and
                    operations.type = 'auto'
                    and
                    operations.deleted = 0
        
                order by operations.id asc
                limit 1
            ) as should_be on true
            where
                should_be.first_auto_number is distinct from public.order.first_auto_number;
        
        
            if invalid_orders is not null then
                raise exception E'\n%, invalid orders:\n%\n\n\noperations:\n%\n\n\n', 
                    check_label,
                    invalid_orders,
                    (
                        select
                        string_agg(
                            '    id: ' || operations.id || E',\n' ||
                            '    id_order: ' || coalesce(operations.id_order::text, '<NULL>') || E',\n' ||
                            '    type: ' || coalesce(operations.type, '<NULL>') || E',\n' ||
                            '    deleted: ' || coalesce(operations.deleted::text, '<NULL>') || E',\n' ||
                            '    doc_number: ' || coalesce(operations.doc_number, '<NULL>') || E',\n' ||
                            '    __first_auto_for_order: ' || coalesce(operations.__first_auto_for_order::text, '<NULL>')
        
                            , E'\n\n'
                        )
                        from operations
                    );
            else
                raise notice '% - success', check_label;
            end if;
        end
        $body$
        language plpgsql;
        
        do $$
        begin
        
            insert into public.order (name) values ('order 1');
            insert into public.order (name) values ('order 2');
        
            insert into operations (doc_number, type, deleted)
            values ('auto 1', 'auto', 0);
            PERFORM check_orders('label 1');
        
            update operations set
                id_order = 1
            where id = 1;
            PERFORM check_orders('label 2');
        
            insert into operations (doc_number, type, deleted, id_order)
            values ('auto 2', 'auto', 0, 1);
            PERFORM check_orders('label 3');
        
            update operations set
                id_order = 2;
            PERFORM check_orders('label 4');
        
            delete from operations;
            PERFORM check_orders('label 5');
        
            insert into operations
                (doc_number, type, deleted, id_order)
            values
                ('auto X', 'auto', 0, 1),
                ('auto Y', 'auto', 0, 1),
                ('auto Z', 'auto', 0, 1),
                ('auto A', 'auto', 0, 2),
                ('auto B', 'auto', 0, 2),
                ('auto C', 'auto', 0, 2)
            ;
            PERFORM check_orders('label 6');
        
            update operations set
                type = 'sea',
                doc_number = 'auto X updated'
            where
                doc_number = 'auto X';
            PERFORM check_orders('label 7');
        
            update operations set
                doc_number = 'auto X update 2'
            where
                doc_number = 'auto X updated';
            PERFORM check_orders('label 8');
        
            update operations set
                type = 'auto',
                doc_number = 'auto X updated 3'
            where
                doc_number = 'auto X updated 2';
            PERFORM check_orders('label 9');
        
            raise exception 'success';
        end
        $$;
        
        `;

        let actualErr: Error = new Error("expected error");
        try {
            await db.query(testSql);
        } catch(err) {
            actualErr = err;
        }

        assert.strictEqual(actualErr.message, "success");
    });

    it("last point by sort desc", async() => {
        const folderPath = ROOT_TMP_PATH + "/last_point_by_sort_desc";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table arrival_points (
                id serial primary key,
                name text,
                id_operation bigint,
                point_name text,
                sort integer
            );
            
            create table operations (
                id serial primary key,
                name text
            );
        `);

        fs.writeFileSync(folderPath + "/last_point.sql", `
            cache last_point for operations (
                select
                    arrival_points.point_name as last_point_name

                from arrival_points
                where
                    arrival_points.id_operation = operations.id

                order by arrival_points.sort desc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const testSql = `

        create or replace function check_operations(check_label text)
        returns void as $body$
        declare invalid_operations text;
        begin
        
            select
                string_agg(
                    '    id: ' || operations.id || E'\n' ||
                    '    name: ' || coalesce(operations.name, '<NULL>') || E'\n' ||
                    '    should_be: ' || coalesce(should_be.last_point_name, '<NULL>') || E'\n' ||
                    '    actual: ' || coalesce(operations.last_point_name, '<NULL>')
        
                    , E'\n\n'
                )
            into
                invalid_operations
        
            from operations
        
            left join lateral (
                select
                    arrival_points.point_name as last_point_name
        
                from arrival_points
                where
                    arrival_points.id_operation = operations.id
        
                order by
                    arrival_points.sort desc,
                    arrival_points.id desc
                limit 1
            ) as should_be on true
            where
                should_be.last_point_name is distinct from operations.last_point_name;
        
        
            if invalid_operations is not null then
                raise exception E'\n%, invalid operations:\n%\n\n\narrival_points:\n%\n\n\n', 
                    check_label,
                    invalid_operations,
                    (
                        select
                        string_agg(
                            '    id: ' || arrival_points.id || E',\n' ||
                            '    name: ' || coalesce(arrival_points.name, '<NULL>') || E',\n' ||
                            '    id_operation: ' || coalesce(arrival_points.id_operation::text, '<NULL>') || E',\n' ||
                            '    sort: ' || coalesce(arrival_points.sort::text, '<NULL>') || E',\n' ||
                            '    point_name: ' || coalesce(arrival_points.point_name, '<NULL>') || E',\n' ||
                            '    __last_point_for_operations: ' || coalesce(arrival_points.__last_point_for_operations::text, '<NULL>')
        
                            , E'\n\n'
                        )
                        from arrival_points
                    );
            else
                raise notice '% - success', check_label;
            end if;
        end
        $body$
        language plpgsql;
        
        do $$
        begin
        
            insert into operations (name) values ('operation 1');
            insert into operations (name) values ('operation 2');
        
            insert into arrival_points
                (name, id_operation, sort, point_name)
            values
                ('point X', 1, 1, 'warehouse 1');
            PERFORM check_operations('label 1 insert');
        
            update arrival_points set
                point_name = 'warehouse 2'
            where name = 'point X';
            PERFORM check_operations('label 2 update point_name');
        
            delete from arrival_points;
            PERFORM check_operations('label 3 delete');
        
            insert into arrival_points
                (name, id_operation, sort, point_name)
            values
                ('point A', 1, 1, 'warehouse 1'),
                ('point B', 1, 2, 'warehouse 2'),
                ('point X', 2, 30, 'warehouse 3'),
                ('point Y', 2, 40, 'warehouse 4')
            ;
            PERFORM check_operations('label 4 insert 4 points');
        
            update arrival_points set
                sort = 0
            where name = 'point A';
            PERFORM check_operations('label 5 update only sort -1 where not last');
        
            update arrival_points set
                sort = 1
            where name = 'point A';
            PERFORM check_operations('label 6 update only sort +1 where not last and after update NOT last');
        
            update arrival_points set
                sort = 3
            where name = 'point A';
            PERFORM check_operations('label 7 update only sort +1 where not last and after update IS last');
        
            update arrival_points set
                sort = 1
            where name = 'point A';
            PERFORM check_operations('label 8 update only sort -1 where is last and after update not last');
        
            update arrival_points set
                id_operation = 2
            where name = 'point B';
            PERFORM check_operations('label 9 update reference where not last for old and not last for new');
        
            update arrival_points set
                id_operation = 1
            where name = 'point Y';
            PERFORM check_operations('label 10 update reference where is last for old and is last for new');
        
            delete from arrival_points;
            PERFORM check_operations('label 11 deleted 4 points');
        
            insert into arrival_points
                (name, id_operation, sort, point_name)
            values
                ('point A', 1, 1, 'warehouse 1'),
                ('point B', 1, 2, 'warehouse 2'),
                ('point X', 2, 30, 'warehouse 3'),
                ('point Y', 2, 40, 'warehouse 4')
            ;
            PERFORM check_operations('label 12 insert 4 points');
        
            update arrival_points set
                id_operation = 2,
                sort = 45
            where name = 'point B';
            PERFORM check_operations('label 13 update sort and reference, where old is last and last for new');
        
            update arrival_points set
                point_name = 'warehouse B',
                sort = 46
            where name = 'point B';
            PERFORM check_operations('label 14 update sort +1 and data, where is last after update sort');
        
            update arrival_points set
                point_name = 'warehouse A',
                sort = 0
            where name = 'point A';
            PERFORM check_operations('label 15 update sort -1 and data, where is last after update sort');
        
            update arrival_points set
                id_operation = 3
            where name = 'point A';
            PERFORM check_operations('label 16 update reference to unknown where is last for OLD');
        
            update arrival_points set
                point_name = 'warehouse A - updated'
            where name = 'point A';
            PERFORM check_operations('label 17 update data where reference is unknown');
        
            update arrival_points set
                point_name = 'warehouse X'
            where name = 'point X';
            PERFORM check_operations('label 18 update data where is not last');
        
            update arrival_points set
                point_name = 'warehouse X - update',
                sort = 100,
                id_operation = 1
            where name = 'point X';
            PERFORM check_operations('label 19 update all where is not last for old and now is last for new');
        
            update arrival_points set
                sort = 200,
                id_operation = 1,
                point_name = 'warehouse A'
            where name = 'point A';
            PERFORM check_operations('label 20 update all where is not last for old and now is last for new');
        
            update arrival_points set
                point_name = 'warehouse X updated twice'
            where name = 'point X';
            PERFORM check_operations('label 21 update data where was last before pvev update');

            delete from arrival_points;
            PERFORM check_operations('label 22 delete all points');

            insert into arrival_points
                (name, id_operation, sort, point_name)
            values
                ('point A', 1, null, 'warehouse 1'),
                ('point B', 1, null, 'warehouse 2'),
                ('point X', 2, null, 'warehouse 3'),
                ('point Y', 2, null, 'warehouse 4')
            ;
            PERFORM check_operations('label 23 insert points with null sort');

            update arrival_points set
                sort = 1
            where
                name in ('point A', 'point B');
            PERFORM check_operations('label 24 update two points from null to 1');

            update arrival_points set
                sort = 2;
            PERFORM check_operations('label 25 update all points sort to 2');
        
            raise exception 'success';
        end
        $$;
        
        `;

        let actualErr: Error = new Error("expected error");
        try {
            await db.query(testSql);
        } catch(err) {
            actualErr = err;
        }

        assert.strictEqual(actualErr.message, "success");
    });

    it("first point by sort asc and filter deleted", async() => {
        const folderPath = ROOT_TMP_PATH + "/first_point_by_sort_asc";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table arrival_points (
                id serial primary key,
                name text,
                id_operation bigint,
                point_name text,
                deleted smallint default 0,
                sort integer not null
            );
            
            create table operations (
                id serial primary key,
                name text
            );
        `);

        fs.writeFileSync(folderPath + "/first_point.sql", `
            cache first_point for operations (
                select
                    first_point.point_name as first_point_name

                from arrival_points as first_point
                where
                    first_point.id_operation = operations.id and
                    first_point.deleted = 0

                order by first_point.sort asc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const testSql = `

        create or replace function check_operations(check_label text)
        returns void as $body$
        declare invalid_operations text;
        begin
        
            select
                string_agg(
                    '    id: ' || operations.id || E'\n' ||
                    '    name: ' || coalesce(operations.name, '<NULL>') || E'\n' ||
                    '    should_be: ' || coalesce(should_be.first_point_name, '<NULL>') || E'\n' ||
                    '    actual: ' || coalesce(operations.first_point_name, '<NULL>')
        
                    , E'\n\n'
                )
            into
                invalid_operations
        
            from operations
        
            left join lateral (
                select
                    first_point.point_name as first_point_name

                from arrival_points as first_point
                where
                    first_point.id_operation = operations.id and
                    first_point.deleted = 0

                order by first_point.sort asc
                limit 1
            ) as should_be on true
            where
                should_be.first_point_name is distinct from operations.first_point_name;
        
        
            if invalid_operations is not null then
                raise exception E'\n%, invalid operations:\n%\n\n\narrival_points:\n%\n\n\n', 
                    check_label,
                    invalid_operations,
                    (
                        select
                        string_agg(
                            '    id: ' || arrival_points.id || E',\n' ||
                            '    name: ' || coalesce(arrival_points.name, '<NULL>') || E',\n' ||
                            '    id_operation: ' || coalesce(arrival_points.id_operation::text, '<NULL>') || E',\n' ||
                            '    sort: ' || coalesce(arrival_points.sort::text, '<NULL>') || E',\n' ||
                            '    point_name: ' || coalesce(arrival_points.point_name, '<NULL>') || E',\n' ||
                            '    __first_point_for_operations: ' || coalesce(arrival_points.__first_point_for_operations::text, '<NULL>')
        
                            , E'\n\n'
                        )
                        from arrival_points
                    );
            else
                raise notice '% - success', check_label;
            end if;
        end
        $body$
        language plpgsql;
        
        
        do $$
        begin

            insert into operations (name) values ('operation 1');
            insert into operations (name) values ('operation 2');

            insert into arrival_points
                (name, id_operation, sort, point_name)
            values
                ('point X', 1, 1, 'warehouse 1');
            PERFORM check_operations('label 1 insert');

            update arrival_points set
                point_name = 'warehouse 2'
            where name = 'point X';
            PERFORM check_operations('label 2 update point_name');

            delete from arrival_points;
            PERFORM check_operations('label 3 delete');

            insert into arrival_points
                (name, id_operation, sort, point_name)
            values
                ('point A', 1, 1, 'warehouse 1'),
                ('point B', 1, 2, 'warehouse 2'),
                ('point X', 2, 30, 'warehouse 3'),
                ('point Y', 2, 40, 'warehouse 4')
            ;
            PERFORM check_operations('label 4 insert 4 points');

            update arrival_points set
                sort = 0
            where name = 'point A';
            PERFORM check_operations('label 5 update only sort -1 where first');

            update arrival_points set
                sort = 1
            where name = 'point A';
            PERFORM check_operations('label 6 update only sort +1 where first and after update first');

            update arrival_points set
                deleted = 1
            where name = 'point A';
            PERFORM check_operations('label 7 update only deleted where first');

            update arrival_points set
                point_name = 'updated point A'
            where name = 'point A';
            PERFORM check_operations('label 8 update data where deleted');

            update arrival_points set
                point_name = 'warehouse A',
                sort = 0
            where name = 'point A';
            PERFORM check_operations('label 9 update data and sort where deleted');

            update arrival_points set
                point_name = 'warehouse A - updated',
                sort = 10,
                deleted = 0
            where name = 'point A';
            PERFORM check_operations('label 10 update deleted = 0 and data and sort, after update not first');

            update arrival_points set
                sort = 1
            where name = 'point A';
            PERFORM check_operations('label 11 update only sort -1, now is first');

            raise exception 'success';
        end
        $$;

        
        `;

        let actualErr: Error = new Error("expected error");
        try {
            await db.query(testSql);
        } catch(err) {
            actualErr = err;
        }

        assert.strictEqual(actualErr.message, "success");
    });

    it("last sea operation by id desc and filter deleted for unit", async() => {
        const folderPath = ROOT_TMP_PATH + "/last_sea_for_unit_by_id";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table units (
                id serial primary key,
                name text
            );

            create table operations (
                id serial primary key,
                name text,
                units_ids bigint[],
                type text,
                deleted smallint,
                incoming_date text,
                outgoing_date text
            );
        `);

        fs.writeFileSync(folderPath + "/last_sea.sql", `
            cache last_sea for units (
                select
                    last_sea.incoming_date as last_sea_incoming_date,
                    last_sea.outgoing_date as last_sea_outgoing_date
            
                from operations as last_sea
                where
                    last_sea.units_ids && array[ units.id ]::bigint[] and
                    last_sea.type = 'sea' and
                    last_sea.deleted = 0
            
                order by
                    last_sea.id desc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const testSql = `

        create or replace function check_units(check_label text)
        returns void as $body$
        declare invalid_units text;
        begin
        
            select
                string_agg(
                    '    id: ' || units.id || E'\n' ||
                    '    name: ' || coalesce(units.name, '<NULL>') || E'\n' ||
                    '    __last_sea_id: ' || coalesce(units.__last_sea_id::text, '<NULL>') || E'\n' ||
                    '    should_be: ' || coalesce(should_be.last_sea_incoming_date, '<NULL>') || E'\n' ||
                    '    actual: ' || coalesce(units.last_sea_incoming_date, '<NULL>')
        
                    , E'\n\n'
                )
            into
                invalid_units
        
            from units
        
            left join lateral (
                select
                    last_sea.id as __last_sea_id,
                    last_sea.incoming_date as last_sea_incoming_date,
                    last_sea.outgoing_date as last_sea_outgoing_date
        
                from operations as last_sea
                where
                    last_sea.units_ids && array[ units.id ]::bigint[] and
                    last_sea.type = 'sea' and
                    last_sea.deleted = 0
        
                order by
                    last_sea.id desc
                limit 1
            ) as should_be on true
            where
                (
                    should_be.last_sea_incoming_date is distinct from units.last_sea_incoming_date
                    or
                    should_be.__last_sea_id is distinct from units.__last_sea_id
                );
        
        
            if invalid_units is not null then
                raise exception E'\n%, invalid units:\n%\n\n\noperations:\n%\n\n\n', 
                    check_label,
                    invalid_units,
                    (
                        select
                        string_agg(
                            '    id: ' || operations.id || E',\n' ||
                            '    name: ' || coalesce(operations.name, '<NULL>') || E',\n' ||
                            '    units_ids: ' || coalesce(operations.units_ids::text, '<NULL>') || E',\n' ||
                            '    type: ' || coalesce(operations.type, '<NULL>') || E',\n' ||
                            '    deleted: ' || coalesce(operations.deleted::text, '<NULL>') || E',\n' ||
                            '    incoming_date: ' || coalesce(operations.incoming_date::text, '<NULL>')
                            , E'\n\n'
                        )
                        from operations
                    );
            else
                raise notice '% - success', check_label;
            end if;
        end
        $body$
        language plpgsql;
        
        do $$
        begin
        
            insert into units (name) values ('unit 1');
            insert into units (name) values ('unit 2');
        
            insert into operations
                (name, type, deleted, units_ids, incoming_date)
            values
                ('sea 1', 'sea', 0, array[1]::bigint[], 'date 1'),
                ('sea 2', 'sea', 0, array[1,2]::bigint[], 'date 2'),
                ('sea 3', 'sea', 0, array[2]::bigint[], 'date 3');
            PERFORM check_units('label 1, insert 3 operations');
        
            update operations set
                deleted = 1
            where name = 'sea 2';
            PERFORM check_units('label 2 update deleted');
        
            update operations set
                units_ids = array[2, 1]
            where name = 'sea 2';
            PERFORM check_units('label 3 update units_ids in deleted operation');
        
            update operations set
                units_ids = array[1, 2],
                deleted = 0
            where name = 'sea 2';
            PERFORM check_units('label 4 update deleted and units_ids in deleted operation');
        
            update operations set
                units_ids = array[2, 1],
                incoming_date = 'date 3 updated'
            where name = 'sea 3';
            PERFORM check_units('label 5 update data and units_ids');
        
            update operations set
                units_ids = array[2],
                incoming_date = 'date 3 update twice'
            where name = 'sea 3';
            PERFORM check_units('label 6 update data and units_ids');
        
            update operations set
                incoming_date = name || ' update all operations';
            PERFORM check_units('label 7 update all operations');
            
            delete from operations;
            PERFORM check_units('label 8 delete all operations');
        
            raise exception 'success';
        end
        $$;
        
        `;

        let actualErr: Error = new Error("expected error");
        try {
            await db.query(testSql);
        } catch(err) {
            actualErr = err;
        }

        assert.strictEqual(actualErr.message, "success");
    });

    it("last sea operation by lvl desc and filter deleted for unit", async() => {
        const folderPath = ROOT_TMP_PATH + "/last_sea_for_unit_by_lvl";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table units (
                id serial primary key,
                name text
            );

            create table operations (
                id serial primary key,
                name text,
                lvl smallint,
                units_ids bigint[],
                type text,
                deleted smallint,
                incoming_date text,
                outgoing_date text
            );
        `);

        fs.writeFileSync(folderPath + "/last_sea.sql", `
            cache last_sea for units (
                select
                    last_sea.incoming_date as last_sea_incoming_date,
                    last_sea.outgoing_date as last_sea_outgoing_date
        
                from operations as last_sea
                where
                    last_sea.units_ids && array[ units.id ]::bigint[] and
                    last_sea.type = 'sea' and
                    last_sea.deleted = 0
        
                order by
                    last_sea.lvl desc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const testSql = `

        create or replace function check_units(check_label text)
        returns void as $body$
        declare invalid_units text;
        begin
        
            select
                string_agg(
                    '    id: ' || units.id || E'\n' ||
                    '    name: ' || coalesce(units.name, '<NULL>') || E'\n' ||
                    '    __last_sea_id: ' || coalesce(units.__last_sea_id::text, '<NULL>') || E'\n' ||
                    '    __last_sea_lvl: ' || coalesce(units.__last_sea_lvl::text, '<NULL>') || E'\n' ||
                    '    incoming_date: should_be: ' || coalesce(should_be.last_sea_incoming_date, '<NULL>') || E'\n' ||
                    '    incoming_date: actual: ' || coalesce(units.last_sea_incoming_date, '<NULL>') || E'\n' ||
                    '    last lvl: should_be: ' || coalesce(should_be.__last_sea_lvl::text, '<NULL>') || E'\n' ||
                    '    last lvl: actual: ' || coalesce(units.__last_sea_lvl::text, '<NULL>') || E'\n' ||
                    '    last id: should_be: ' || coalesce(should_be.__last_sea_id::text, '<NULL>') || E'\n' ||
                    '    last id: actual: ' || coalesce(units.__last_sea_id::text, '<NULL>')
        
                    , E'\n\n'
                )
            into
                invalid_units
        
            from units
        
            left join lateral (
                select
                    last_sea.id as __last_sea_id,
                    last_sea.lvl as __last_sea_lvl,
                    last_sea.incoming_date as last_sea_incoming_date,
                    last_sea.outgoing_date as last_sea_outgoing_date
        
                from operations as last_sea
                where
                    last_sea.units_ids && array[ units.id ]::bigint[] and
                    last_sea.type = 'sea' and
                    last_sea.deleted = 0
        
                order by
                    last_sea.lvl desc,
                    last_sea.id desc
                limit 1
            ) as should_be on true
            where
                (
                    should_be.last_sea_incoming_date is distinct from units.last_sea_incoming_date
                    or
                    should_be.__last_sea_id is distinct from units.__last_sea_id
                    or
                    should_be.__last_sea_lvl is distinct from units.__last_sea_lvl
                );
        
        
            if invalid_units is not null then
                raise exception E'\n%, invalid units:\n%\n\n\noperations:\n%\n\n\n', 
                    check_label,
                    invalid_units,
                    (
                        select
                        string_agg(
                            '    id: ' || operations.id || E',\n' ||
                            '    name: ' || coalesce(operations.name, '<NULL>') || E',\n' ||
                            '    lvl: ' || coalesce(operations.lvl::text, '<NULL>') || E',\n' ||
                            '    units_ids: ' || coalesce(operations.units_ids::text, '<NULL>') || E',\n' ||
                            '    type: ' || coalesce(operations.type, '<NULL>') || E',\n' ||
                            '    deleted: ' || coalesce(operations.deleted::text, '<NULL>') || E',\n' ||
                            '    incoming_date: ' || coalesce(operations.incoming_date::text, '<NULL>')
                            , E'\n\n'
                        )
                        from operations
                    );
            else
                raise notice '% - success', check_label;
            end if;
        end
        $body$
        language plpgsql;
        
        do $$
        begin
        
            insert into units (name) values ('unit 1');
            insert into units (name) values ('unit 2');
        
            insert into operations
                (name, lvl, type, deleted, units_ids, incoming_date)
            values
                ('sea 1', null, 'sea', 0, array[1]::bigint[], 'date 1'),
                ('sea 2', null, 'sea', 0, array[1,2]::bigint[], 'date 2'),
                ('sea 3', null, 'sea', 0, array[2]::bigint[], 'date 3');
            PERFORM check_units('label 1, insert 3 operations without lvl');
        
            delete from operations;
            PERFORM check_units('label 2, delete 3 operations without lvl');
        
        
            insert into operations
                (name, lvl, type, deleted, units_ids, incoming_date)
            values
                ('sea 1', 1, 'sea', 0, array[1]::bigint[], 'date 1'),
                ('sea 2', 2, 'sea', 0, array[1,2]::bigint[], 'date 2'),
                ('sea 3', 3, 'sea', 0, array[2]::bigint[], 'date 3');
            PERFORM check_units('label 3, insert 3 operations with lvl');
        
            update operations set
                lvl = 4,
                incoming_date = 'date 2 updated',
                units_ids = array[2, 1]::bigint[]
            where name = 'sea 2';
            PERFORM check_units('label 4, update lvl to max + 1 and change data');
        
            update operations set
                lvl = 0,
                incoming_date = 'date 2',
                units_ids = array[2]::bigint[]
            where name = 'sea 2';
            PERFORM check_units('label 5, update max lvl to min and change reference and change data');
        
            update operations set
                units_ids = array[2, 1]::bigint[]
            where name = 'sea 2';
            PERFORM check_units('label 6, update reference in min lvl');
        
            update operations set
                lvl = 2
            where name = 'sea 2';
            PERFORM check_units('label 7, update only lvl from min to medium');
        
            update operations set
                lvl = null
            where name = 'sea 3';
            PERFORM check_units('label 8, update lvl=null where lvl was max');
        
            update operations set
                units_ids = array[1]::bigint[]
            where name = 'sea 3';
            PERFORM check_units('label 9, update units_ids=[1] where lvl is null');
        
            update operations set
                units_ids = array[2]::bigint[]
            where name = 'sea 3';
            PERFORM check_units('label 10, update units_ids=[2] where lvl is null');
        
            delete from operations;
            PERFORM check_units('label 11, delete from operations');
        
            insert into operations
                (name, lvl, type, deleted, units_ids, incoming_date)
            values
                ('sea 1', 1, 'sea', 0, array[1]::bigint[], null),
                ('sea 2', 2, 'sea', 0, array[2]::bigint[], null);
            PERFORM check_units('label 12, insert 2 operations with not null lvl and null dates');

            insert into operations
                (name, lvl, type, deleted, units_ids, incoming_date)
            values
                ('sea 3', null, 'sea', 0, array[1]::bigint[], null),
                ('sea 4', null, 'sea', 0, array[2]::bigint[], null);
            PERFORM check_units('label 13, insert 2 operations with null lvl and null dates');

            update operations set
                incoming_date = 'date';
            PERFORM check_units('label 14, update all dates to same date');

            update operations set
                units_ids = array[1, 2];
            PERFORM check_units('label 15, update all units_ids');

            update operations set
                incoming_date = 'date - ' || operations.name;
            PERFORM check_units('label 16, update all dates to different dates');

            raise exception 'success';
        end
        $$;
        
        `;

        let actualErr: Error = new Error("expected error");
        try {
            await db.query(testSql);
        } catch(err) {
            actualErr = err;
        }

        assert.strictEqual(actualErr.message, "success");
    });

    it("test insert new ids to array reference and change sum", async() => {
        const folderPath = ROOT_TMP_PATH + "/deleted_and_orders_ids";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                name text
            );
            
            create table invoices (
                id serial primary key,
                name text,
                deleted smallint default 0,
                orders_ids bigint[],
                profit numeric
            );
        `);

        fs.writeFileSync(folderPath + "/invoices.sql", `
            cache invoices for orders (
                select
                    sum( invoices.profit ) as invoices_profit

                from invoices
                where
                    invoices.orders_ids && ARRAY[orders.id]::bigint[] and
                    invoices.deleted = 0
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        const testSql = `

        create or replace function check_units(check_label text)
        returns void as $body$
        declare invalid_orders text;
        begin
        
            select
                string_agg(
                    '    id: ' || orders.id || E'\n' ||
                    '    name: ' || coalesce(orders.name, '<NULL>') || E'\n' ||
                    '    profit: should_be: ' || coalesce(should_be.invoices_profit::text, '<NULL>') || E'\n' ||
                    '    profit: actual: ' || coalesce(orders.invoices_profit::text, '<NULL>')
        
                    , E'\n\n'
                )
            into
                invalid_orders
        
            from orders
        
            left join lateral (
                select
                    sum( invoices.profit ) as invoices_profit
        
                from invoices
                where
                    invoices.orders_ids && array[ orders.id ]::bigint[] and
                    invoices.deleted = 0
            ) as should_be on true
            where
                coalesce(should_be.invoices_profit, 0) != coalesce(orders.invoices_profit, 0);
        
        
            if invalid_orders is not null then
                raise exception E'\n%, invalid orders:\n%\n\n\ninvoices:\n%\n\n\n', 
                    check_label,
                    invalid_orders,
                    (
                        select
                        string_agg(
                            '    id: ' || invoices.id || E',\n' ||
                            '    name: ' || coalesce(invoices.name, '<NULL>') || E',\n' ||
                            '    orders_ids: ' || coalesce(invoices.orders_ids::text, '<NULL>') || E',\n' ||
                            '    deleted: ' || coalesce(invoices.deleted::text, '<NULL>') || E',\n' ||
                            '    profit: ' || coalesce(invoices.profit::text, '<NULL>')
                            , E'\n\n'
                        )
                        from invoices
                    );
            else
                raise notice '% - success', check_label;
            end if;
        end
        $body$
        language plpgsql;
        
        do $$
        begin
        
            insert into orders (name) values ('order 1');
            insert into orders (name) values ('order 2');
        
            PERFORM check_units('label 1, insert 2 orders');
        
            insert into invoices
                (name, orders_ids, profit)
            values
                ('invoice 1', ARRAY[1], 100),
                ('invoice 2', ARRAY[2], 250);
            PERFORM check_units('label 2, insert 2 invoices');
        
            delete from invoices;
            PERFORM check_units('label 3, delete 2 invoices');
        
            insert into invoices
                (name, orders_ids, profit)
            values
                ('invoice 1', ARRAY[1], 99),
                ('invoice 2', ARRAY[2], 250),
                ('invoice 3', ARRAY[1,2], 1000);
            PERFORM check_units('label 4, insert 3 invoices');
        
            update invoices set
                profit = profit - 5;
            PERFORM check_units('label 5, update all invoices, set profit -= 5');
        
            update invoices set
                profit = profit + 5;
            PERFORM check_units('label 6, update all invoices, set profit += 5');
        
        
            update invoices set
                deleted = 1
            where name = 'invoice 3';
            PERFORM check_units('label 7, update last invoice, set deleted = 1');
        
            update invoices set
                deleted = 0,
                profit = 2000
            where name = 'invoice 3';
            PERFORM check_units('label 8, return back deleted invoice and change him profit');
        
            update invoices set
                orders_ids = orders_ids || array[0]::bigint[];
            PERFORM check_units('label 9, add unknown order id to all invoices');
        
            update invoices set
                orders_ids = array[1, 2];
            PERFORM check_units('label 10, link all invoices with all orders');
        
            update invoices set
                orders_ids = array[1],
                deleted = 1;
            PERFORM check_units('label 11, all invoices set deleted = 1 and link with only first order');
        
            update invoices set
                orders_ids = array[1, 2],
                deleted = 0,
                profit = 200
            ;
            PERFORM check_units('label 12, all invoices and all columns');
        
            raise exception 'success';
        end
        $$;
        
        `;

        let actualErr: Error = new Error("expected error");
        try {
            await db.query(testSql);
        } catch(err) {
            actualErr = err;
        }

        assert.strictEqual(actualErr.message, "success");
    });

    it("test one row trigger", async() => {
        const folderPath = ROOT_TMP_PATH + "/one_row_working";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table user_task (
                id serial primary key,
                name text,
                query_name text,
                row_id bigint,
                deleted smallint default 0
            );
            
            create table comments (
                id serial primary key,
                name text,
                query_name text,
                row_id bigint,
                user_task_query_name text,
                user_task_row_id bigint
            );
        `);

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
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        const testSql = `

        create or replace function check_cache(check_label text)
        returns void as $body$
        declare invalid_rows text;
        begin
        
            select
                string_agg(
                    '    id: ' || comments.id || E'\n' ||
                    '    name: ' || coalesce(comments.name, '<NULL>') || E'\n' ||
                    '    user_task_query_name: should_be: ' || coalesce(should_be.user_task_query_name::text, '<NULL>') || E'\n' ||
                    '    user_task_query_name: actual: ' || coalesce(comments.user_task_query_name::text, '<NULL>') || E'\n' ||
                    '    user_task_row_id: should_be: ' || coalesce(should_be.user_task_query_name::text, '<NULL>') || E'\n' ||
                    '    user_task_row_id: actual: ' || coalesce(comments.user_task_row_id::text, '<NULL>')
        
                    , E'\n\n'
                )
            into
                invalid_rows
        
            from comments
        
            left join lateral (
                select
                    user_task.query_name as user_task_query_name,
                    user_task.row_id as user_task_row_id
        
                from user_task
                where
                    user_task.id = comments.row_id and
                    user_task.deleted = 0 and
                    comments.query_name = 'USER_TASK'
            ) as should_be on true
            where
                (
                    should_be.user_task_query_name is distinct from comments.user_task_query_name
                    or
                    should_be.user_task_row_id is distinct from comments.user_task_row_id
                );
        
        
            if invalid_rows is not null then
                raise exception E'\n%, invalid cache rows:\n%\n\n\nsource rows:\n%\n\n\n', 
                    check_label,
                    invalid_rows,
                    (
                        select
                        string_agg(
                            '    id: ' || user_task.id || E',\n' ||
                            '    name: ' || coalesce(user_task.name, '<NULL>') || E',\n' ||
                            '    query_name: ' || coalesce(user_task.query_name::text, '<NULL>') || E',\n' ||
                            '    row_id: ' || coalesce(user_task.row_id::text, '<NULL>') || E',\n' ||
                            '    deleted: ' || coalesce(user_task.deleted::text, '<NULL>')
                            , E'\n\n'
                        )
                        from user_task
                    );
            else
                raise notice '% - success', check_label;
            end if;
        end
        $body$
        language plpgsql;
        
        do $$
        begin
        
            insert into user_task
                (name, query_name, row_id, deleted)
            values
                ('task 1', 'ORDER', 10, 0);
        
            insert into comments
                (name, query_name, row_id)
            values
                ('comment 1', 'USER_TASK', 1),
                ('comment 2', 'USER_TASK', 2),
                ('comment 3', 'ORDER', 1);
        
            PERFORM check_cache('label 1, insert 1 task and 3 comments');
        
            update comments set
                query_name = 'USER_TASK'
            where name = 'comment 3';
            PERFORM check_cache('label 2, updated comment query_name');
        
            update user_task set
                deleted = 1
            where name = 'task 1';
            PERFORM check_cache('label 3, updated user_task deleted');
        
            insert into user_task
                (name, query_name, row_id, deleted)
            values
                ('task 2', 'ORDER', 20, 0);
            PERFORM check_cache('label 4, insert new user_task');
        
            delete from user_task;
            PERFORM check_cache('label 5, delete user_task');
        
        
            raise exception 'success';
        end
        $$;
        
        
        `;

        let actualErr: Error = new Error("expected error");
        try {
            await db.query(testSql);
        } catch(err) {
            actualErr = err;
        }

        assert.strictEqual(actualErr.message, "success");
    });

    it("when exists before update trigger who change dependency columns ", async() => {
        const folderPath = ROOT_TMP_PATH + "/one_row_working";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );
            
            create table fin_operation (
                id serial primary key,
                id_order bigint,
                debit numeric,
                credit numeric,
                total numeric
            );

            create or replace function debit_credit_total()
            returns trigger as $body$
            begin
                
                new.total = coalesce(new.debit, -new.credit);
                
                return new;
            end
            $body$
            language plpgsql;

            create trigger debit_credit_total
            before insert or update of debit, credit
            on fin_operation
            for each row
            execute procedure debit_credit_total();

            insert into orders default values;
            insert into fin_operation
                (id_order, debit, credit)
            values
                (1, 100, null);
        `);

        fs.writeFileSync(folderPath + "/fin_totals.sql", `
            cache fin_totals for orders (
                select
                    sum( fin_operation.total ) as fin_total

                from fin_operation
                where
                    fin_operation.id_order = orders.id
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            update fin_operation set
                credit = 400,
                debit = null;
        `);


        const {rows} = await db.query(`
            select fin_total from orders
        `);
        assert.deepStrictEqual(rows[0], {
            fin_total: "-400"
        })
    });

    it("insert into table with commutative trigger with reference by id", async() => {
        const folderPath = ROOT_TMP_PATH + "/one_row_working";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table company (
                id serial primary key,
                id_parent bigint,
                profit numeric,
                deleted smallint default 0
            );

            insert into company default values;
        `);

        fs.writeFileSync(folderPath + "/children_ids.sql", `
            cache children_ids for company as parent_company (
                select
                    array_agg( child_company.id ) as children_ids

                from company as child_company
                where
                    child_company.id_parent = parent_company.id and
                    child_company.deleted = 0
            )
        `);

        fs.writeFileSync(folderPath + "/children_profit.sql", `
            cache children_profit for company as parent_company (
                select
                    sum( child_company.profit ) as children_profit

                from company as child_company
                where
                    child_company.id = any( parent_company.children_ids )
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        let result;

        // insert child
        await db.query(`
            insert into company
                (id_parent, profit)
            values
                (1, 1000)
        `);
        result = await db.query(`
            select children_profit
            from company
            where id = 1
        `);
        assert.deepStrictEqual(result.rows[0], {
            children_profit: "1000"
        });


        // update child, set deleted = 1
        await db.query(`
            update company set
                deleted = 1
            where id = 2
        `);
        result = await db.query(`
            select children_profit
            from company
            where id = 1
        `);
        assert.deepStrictEqual(result.rows[0], {
            children_profit: null
        })
    });

    it("fill columns for coalesce(bool_or())", async() => {
        const folderPath = ROOT_TMP_PATH + "/coalesce_bool_or";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table weight_position (
                id serial primary key
            );
            create table weight_position_link (
                id serial primary key,
                id_position bigint,
                weight numeric
            );

            insert into weight_position default values;
            insert into weight_position default values;

            insert into weight_position_link
                (id_position, weight)
            values
                (1, 55),
                (2, null);
        `);

        fs.writeFileSync(folderPath + "/bool_or.sql", `
            cache has_weight for weight_position as position (
                select
                    coalesce(
                        bool_or(link.weight is not null),
                        false
                    ) as has_weight
            
                from weight_position_link as link
                where
                    link.id_position = position.id
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const {rows} = await db.query(`
            select id, has_weight
            from weight_position
            order by id
        `);
        assert.deepStrictEqual(rows, [
            {id: 1, has_weight: true},
            {id: 2, has_weight: false}
        ]);
    });

    it("string_agg with hard expression by joins", async() => {
        const folderPath = ROOT_TMP_PATH + "/coalesce_bool_or";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table operation (
                id serial primary key
            );
            create table arrival_point (
                id serial primary key,
                id_operation bigint not null,
                id_country bigint,
                id_point bigint,
                sort integer not null
            );
            create table list_country (
                id serial primary key,
                code text
            );
            create table list_warehouse (
                id serial primary key,
                list_warehouse_name text
            );

            insert into list_country (code) values ('RU');
            insert into list_country (code) values ('EN');
            insert into list_warehouse (list_warehouse_name) values ('AAA');
            insert into list_warehouse (list_warehouse_name) values ('BBB');

            insert into operation default values;
        `);

        fs.writeFileSync(folderPath + "/route_text.sql", `
            cache route_text for operation as oper (

                SELECT
                    string_agg(
                        
                        --// !!! конкатенация с null вернет null !!!
                        --// '(страна) ' или ничего
                        coalesce('(' || trim(country.code) || ') ',
                                '') ||
                            --// 'точка' или ничего
                            coalesce(
                                trim(place.list_warehouse_name), '')
            
                        , ' -> '
            
                        order by arr_point.sort asc
            
                    ) AS route_text
            
                FROM arrival_point as arr_point
            
                LEFT JOIN list_country as country ON
                    country.id = arr_point.id_country AND
                    country.code IS NOT NULL AND
                    trim(country.code) <> ''
            
                LEFT JOIN list_warehouse as place ON
                    place.id = arr_point.id_point AND
                    place.list_warehouse_name IS NOT NULL AND
                    trim(place.list_warehouse_name) <> ''
            
                WHERE
                    arr_point.id_operation = oper.id
                    AND arr_point.id_point IS NOT NULL
            )
            without triggers on list_country
            without triggers on list_warehouse
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            insert into arrival_point (
                id_operation, id_country, id_point, sort
            ) values 
                (1, 1, 1, 1),
                (1, 2, 2, 2)
            ;
        `);

        const {rows} = await db.query(`
            select id, route_text 
            from operation
        `);
        assert.deepStrictEqual(rows, [
            { id: 1, route_text: "(RU) AAA -> (EN) BBB" }
        ]);
    });

    it("count with filter by mutable column", async() => {
        const folderPath = ROOT_TMP_PATH + "/count_filter";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );
            create table gtds (
                id serial primary key,
                orders_ids integer[],
                date_clear timestamp without time zone
            );

            insert into orders default values;
            insert into gtds (orders_ids) values (array[1]);
            insert into gtds (orders_ids) values (array[1]);
            insert into gtds (orders_ids) values (array[1]);
        `);

        fs.writeFileSync(folderPath + "/route_text.sql", `
            cache gtds for orders (

                SELECT
                    count(*) as all_count,
                    count(*) filter ( where
                        gtds.date_clear is not null
                    ) as clear_count
            
                FROM gtds
            
                WHERE
                    gtds.orders_ids && array[ orders.id ]
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            update gtds set
                date_clear = now()
            where
                id = 3
        `);

        let actual: any;

        actual = await db.query(`
            select id, all_count, clear_count
            from orders
        `);
        assert.deepStrictEqual(actual.rows, [
            { id: 1, all_count: "3", clear_count: "1" }
        ]);


        await db.query(`
            update gtds set
                date_clear = case 
                    when id in (1,2)
                    then now() 
                    else null 
                end
        `);
        actual = await db.query(`
            select id, all_count, clear_count
            from orders
        `);
        assert.deepStrictEqual(actual.rows, [
            { id: 1, all_count: "3", clear_count: "2" }
        ]);
    });


    it("first_point (order by ASC trigger): parallel update sort", async() => {
        const folderPath = ROOT_TMP_PATH + "/count_filter";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table operations (
                id serial primary key
            );
            create table arrival_points (
                id serial primary key,
                id_operation integer,
                sort integer,
                point_name text
            );

            insert into operations (id) values (1);
            insert into arrival_points
                (id, id_operation, sort, point_name)
            values
                (1, 1, 10, 'A'),
                (2, 1, 20, 'B'),
                (3, 1, 30, 'C');
        `);

        fs.writeFileSync(folderPath + "/first_point_name.sql", `
            cache first_point for operations (
                select
                    arrival_points.point_name as first_point_name

                from arrival_points
                where
                    arrival_points.id_operation = operations.id

                order by arrival_points.sort asc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const transaction1 = await getDBClient();
        const transaction2 = await getDBClient();

        await transaction1.query("begin");
        await transaction2.query("begin");

        await transaction1.query("update arrival_points set sort = 14 where id = 3;");

        let doneTransactions!: (...args: any[]) => void;
        transaction2.query("update arrival_points set sort = 15 where id = 1;")
            .then(async() => {
                await transaction2.query("commit");
    
                await sleep(30);
                while ( !doneTransactions ) {
                    await sleep(10);
                }
                doneTransactions();
            });
            
        await sleep(100);
        await transaction1.query("commit");
        
        await new Promise((resolve) => {
            doneTransactions = resolve;
        });

        const {rows} = await db.query(`select id, first_point_name from operations`);
        assert.deepStrictEqual(rows, [
            {id: 1, first_point_name: "C"}
        ]);

        await transaction1.end();
        await transaction2.end();
    });

    it("first_point (order by ASC trigger): parallel insert and update sort", async() => {
        const folderPath = ROOT_TMP_PATH + "/count_filter";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table operations (
                id serial primary key
            );
            create table arrival_points (
                id serial primary key,
                id_operation integer,
                sort integer,
                point_name text
            );

            insert into operations (id) values (1);
            insert into arrival_points
                (id, id_operation, sort, point_name)
            values
                (1, 1, 10, 'A'),
                (2, 1, 20, 'B'),
                (3, 1, 30, 'C');
        `);

        fs.writeFileSync(folderPath + "/first_point_name.sql", `
            cache first_point for operations (
                select
                    arrival_points.point_name as first_point_name

                from arrival_points
                where
                    arrival_points.id_operation = operations.id

                order by arrival_points.sort asc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const transaction1 = await getDBClient();
        const transaction2 = await getDBClient();

        await transaction1.query("begin");
        await transaction2.query("begin");

        await transaction1.query(`
            insert into arrival_points 
                (id, id_operation, sort, point_name) 
            values
                (4, 1, 15, 'D');
        `);

        let doneTransactions!: (...args: any[]) => void;
        transaction2.query("update arrival_points set sort = 25 where id = 1;")
            .then(async() => {
                await transaction2.query("commit");
    
                await sleep(30);
                while ( !doneTransactions ) {
                    await sleep(10);
                }
                doneTransactions();
            });
            
        await sleep(100);
        await transaction1.query("commit");
        
        await new Promise((resolve) => {
            doneTransactions = resolve;
        });

        const {rows} = await db.query(`select id, first_point_name from operations`);
        assert.deepStrictEqual(rows, [
            {id: 1, first_point_name: "D"}
        ]);

        await transaction1.end();
        await transaction2.end();
    });

    it("first operation for order, when order has operations_ids", async() => {
        const folderPath = ROOT_TMP_PATH + "/last_sea_for_unit_by_lvl";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                operations_ids integer[]
            );

            create table operations (
                id serial primary key,
                name text,
                lvl integer,
                deleted smallint default 0
            );
        `);

        fs.writeFileSync(folderPath + "/first_oper.sql", `
            cache first_operation for orders (
                select
                    first_oper.name as first_oper_name
        
                from operations as first_oper
                where
                    first_oper.id = any( orders.operations_ids ) and
                    first_oper.deleted = 0
        
                order by first_oper.lvl
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        
        await db.query(`
            insert into orders default values;
            insert into orders default values;
            insert into orders default values;

            insert into operations
                (name, lvl)
            values
                ('sea 1', 1),
                ('sea 2', 2),
                ('auto 3', 3),
                ('auto 4', 4)
            ;
        `);

        let actual: any;

        await db.query(`
            update orders set
                operations_ids = array[1, 3]
            where id = 1;
            update orders set
                operations_ids = array[2, 4]
            where id = 2;
            update orders set
            operations_ids = array[3, 4]
            where id = 3;
        `);
        actual = await db.query(`
            select id, first_oper_name
            from orders
            order by id
        `);
        assert.deepStrictEqual(actual.rows, [
            { id: 1, first_oper_name: "sea 1" },
            { id: 2, first_oper_name: "sea 2" },
            { id: 3, first_oper_name: "auto 3" }
        ]);


        await db.query(`
            update operations set
                name = name || ' updated'
        `);
        actual = await db.query(`
            select id, first_oper_name
            from orders
            order by id
        `);
        assert.deepStrictEqual(actual.rows, [
            { id: 1, first_oper_name: "sea 1 updated" },
            { id: 2, first_oper_name: "sea 2 updated" },
            { id: 3, first_oper_name: "auto 3 updated" }
        ]);
    });

    async function sleep(ms: number) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

});