import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../../getDbClient";
import { DDLManager } from "../../../../lib/DDLManager";

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/DDLManager.test cache", () => {
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
        await db.end();
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
                id serial primary key,
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
            max_or_null_sea_date: null
        });

        // test set deleted = 1
        await db.query(`
            update operation.unit set
                deleted = 1
        `);
        await testOrder({
            id: 1,
            max_or_null_sea_date: null
        });

        // test insert two units
        await db.query(`
            insert into operation.unit (id_order)
            values (1), (1);
        `);
        await testOrder({
            id: 1,
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
                max_or_null_sea_date
            from public.order
        `);
        await testOrder({
            id: 1,
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
            max_or_null_sea_date: someDate
        });

        // test insert third unit
        await db.query(`
            insert into operation.unit (id_order)
            values (1)
        `);
        await testOrder({
            id: 1,
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
            max_or_null_sea_date: null
        });


        async function testOrder(expectedRow: {
            id: number,
            max_or_null_sea_date: string | null
        }) {
            result = await db.query(`
                select
                    id,
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
            sum_red: null,
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
                    '    should_be: ' || coalesce(should_be.last_sea_incoming_date, '<NULL>') || E'\n' ||
                    '    actual: ' || coalesce(units.last_sea_incoming_date, '<NULL>')
        
                    , E'\n\n'
                )
            into
                invalid_units
        
            from units
        
            left join lateral (
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
            ) as should_be on true
            where
                should_be.last_sea_incoming_date is distinct from units.last_sea_incoming_date;
        
        
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
                    '    incoming_date: should_be: ' || coalesce(should_be.last_sea_incoming_date, '<NULL>') || E'\n' ||
                    '    incoming_date: actual: ' || coalesce(units.last_sea_incoming_date, '<NULL>') || E'\n' ||
                    '    outgoing_date: should_be: ' || coalesce(should_be.last_sea_outgoing_date, '<NULL>') || E'\n' ||
                    '    incoming_date: actual: ' || coalesce(units.last_sea_outgoing_date, '<NULL>') || E'\n'
        
                    , E'\n\n'
                )
            into
                invalid_units
        
            from units
        
            left join lateral (
                select
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
                    should_be.last_sea_outgoing_date is distinct from units.last_sea_outgoing_date
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
                            '    incoming_date: ' || coalesce(operations.incoming_date::text, '<NULL>') || E',\n' ||
                            '    outgoing_date: ' || coalesce(operations.outgoing_date::text, '<NULL>')
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

    it("two caches, one by join, another just array_agg", async() => {
        const folderPath = ROOT_TMP_PATH + "/test_two_cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table invoice (
                id serial primary key,
                vector integer not null 
                    check (vector IN (1, -1))
            );

            create table payment (
                id serial primary key
            );

            create table invoice_position (
                id serial primary key,
                id_invoice integer references invoice,
                id_payment integer references payment,
                sum numeric
            );


        `);

        fs.writeFileSync(folderPath + "/invoice.sql", `
            cache invoice for invoice_position (
                select
                    invoice.vector as invoice_vector
        
                from invoice
                where
                    invoice.id = invoice_position.id_invoice
            )
        `);

        fs.writeFileSync(folderPath + "/invoices.sql", `
            cache invoices for payment (
                select
                    sum(
                        invoice_position.sum * 
                        invoice_position.invoice_vector 
                    ) as invoices_sum,

                    array_agg( invoice_position.id_invoice ) as invoices_ids
        
                from invoice_position
                where
                    invoice_position.id_payment = payment.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        
        await db.query(`
            insert into payment default values;
            insert into invoice (vector) values (1);

            insert into invoice_position
                (id_invoice, id_payment, sum)
            values
                (1, 1, 100)
            ;
        `);

        const actual = await db.query(`
            select invoices_sum, invoices_ids
            from payment
        `);
        assert.deepStrictEqual(actual.rows, [
            { invoices_sum: "100", invoices_ids: [1] }
        ]);
    });

    it("two caches, one by agg from one table, another just array_agg from other table", async() => {
        const folderPath = ROOT_TMP_PATH + "/test_two_cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table payment (
                id serial primary key
            );

            create table invoice_position (
                id serial primary key,
                id_invoice integer,
                incoming_invoice_positions_ids integer[],
                id_payment integer references payment,
                sum numeric
            );

        `);

        fs.writeFileSync(folderPath + "/incoming.sql", `
            cache incoming for invoice_position as outgoing_position (
                select
                    sum( incoming_position.sum ) as incoming_sum
        
                from invoice_position as incoming_position
                where
                    incoming_position.id = any(outgoing_position.incoming_invoice_positions_ids)
            )
        `);

        fs.writeFileSync(folderPath + "/invoices.sql", `
            cache invoices for payment (
                select
                    sum(
                        coalesce(invoice_position.sum, 0) - 
                        coalesce(invoice_position.incoming_sum, 0)
                    ) as invoices_sum,

                    array_agg( invoice_position.id_invoice ) as invoices_ids
        
                from invoice_position
                where
                    invoice_position.id_payment = payment.id and
                    invoice_position.incoming_invoice_positions_ids is not null
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        
        await db.query(`
            insert into payment default values;

            insert into invoice_position
                (id_payment, id_invoice, sum, incoming_invoice_positions_ids)
            values
                (1, 1, 100, null),
                (1, 1, 50, null),
                (1, 1, 610, array[1, 2])
            ;
        `);

        const actual = await db.query(`
            select invoices_sum, invoices_ids
            from payment
        `);
        assert.deepStrictEqual(actual.rows, [
            { invoices_sum: "460", invoices_ids: [1] }
        ]);
    });

    it("two caches, one self calc, another just array_agg from other table", async() => {
        const folderPath = ROOT_TMP_PATH + "/test_two_cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table payment (
                id serial primary key
            );

            create table invoice_position (
                id serial primary key,
                id_invoice integer,
                id_payment integer references payment,
                debit numeric,
                credit numeric
            );

        `);

        fs.writeFileSync(folderPath + "/a_profit.sql", `
            cache a_profit for invoice_position (
                select
                    (
                        coalesce(invoice_position.debit, 0) -
                        coalesce(invoice_position.credit, 0)
                    ) as profit
            )
        `);

        fs.writeFileSync(folderPath + "/invoices.sql", `
            cache invoices for payment (
                select
                    sum( invoice_position.profit ) as invoices_profit,

                    array_agg( invoice_position.id_invoice ) as invoices_ids
        
                from invoice_position
                where
                    invoice_position.id_payment = payment.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        
        await db.query(`
            insert into payment default values;

            insert into invoice_position
                (id_payment, id_invoice, debit, credit)
            values
                (1, 1, 100, null),
                (1, 2, null, 60)
            ;
        `);

        const actual = await db.query(`
            select invoices_profit, invoices_ids
            from payment
        `);
        assert.deepStrictEqual(actual.rows, [
            { invoices_profit: "40", invoices_ids: [1, 2] }
        ]);
    });

    it("two caches, one-row cache dependent on self cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/test_two_cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table documents (
                id serial primary key,
                doc_number text
            );

            create table orders (
                id serial primary key,
                id_invoice_doc integer
                    references documents,
                id_corrected_invoice_doc integer
                    references documents
            );
        `);

        fs.writeFileSync(folderPath + "/z_doc.sql", `
            cache z_doc for orders (
                select
                    coalesce(
                        orders.id_corrected_invoice_doc,
                        orders.id_invoice_doc
                    ) as id_total_invoice_doc
            )
        `);

        fs.writeFileSync(folderPath + "/total_invoice.sql", `
            cache total_invoice for orders (
                select
                    documents.doc_number as total_invoice_doc_number
                from documents
                where
                    documents.id = orders.id_total_invoice_doc
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        
        await db.query(`
            insert into documents (doc_number)
            values ('old invoice'), ('new invoice');

            insert into orders
                (id_invoice_doc, id_corrected_invoice_doc)
            values
                (1, 2)
            ;
        `);

        const actual = await db.query(`
            select total_invoice_doc_number
            from orders
        `);
        assert.deepStrictEqual(actual.rows, [
            { total_invoice_doc_number: "new invoice" }
        ]);
    });

    it("two caches, one of commutative columns dependent on self cache", async() => { 
        const folderPath = ROOT_TMP_PATH + "/test_two_cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                discount numeric default 1,
                invoices_ids integer[]
            );

            create table invoices (
                id serial primary key,
                credit numeric,
                debit numeric
            );
        `);

        fs.writeFileSync(folderPath + "/z.sql", `
            cache z for orders (
                select
                    orders.invoices_credit * orders.discount 
                        as invoices_credit_with_discount
            )
        `);

        fs.writeFileSync(folderPath + "/total_invoice.sql", `
            cache total_invoice for orders (
                select
                    sum( invoices.credit ) as invoices_credit,
                    sum( invoices.debit ) - orders.invoices_credit_with_discount
                        as invoices_profit

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
            insert into invoices
                (credit, debit)
            values
                (1000, null),
                (null, 1000)
            ;

            insert into orders (discount, invoices_ids)
            values (0.9, array[1, 2]);
        `);

        let actual = await db.query(`
            select invoices_profit
            from orders
        `);
        assert.deepStrictEqual(actual.rows, [
            { invoices_profit: "100.0" }
        ]);


        await db.query(`
            update invoices set
                credit = credit * 2,
                debit = debit * 2;
        `);
        actual = await db.query(`
            select invoices_profit
            from orders
        `);
        assert.deepStrictEqual(actual.rows, [
            { invoices_profit: "200.0" }
        ]);
    });

    it("two caches, one of commutative columns dependent on self cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/test_two_cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );

            create table units (
                id serial primary key,
                id_order integer
            );

            create table declarations (
                id serial primary key,
                start_date date,
                finish_date date,
                id_unit integer
            );
        `);

        fs.writeFileSync(folderPath + "/a_unit_declarations.sql", `
            cache a_unit_declarations for units (
                select
                    min( declarations.start_date ) as min_declaration_start_date,
                    max( declarations.finish_date ) as max_declaration_finish_date

                from declarations
                where
                    declarations.id_unit = units.id
            )
        `);

        fs.writeFileSync(folderPath + "/b_unit_declaration_status.sql", `
            cache b_unit_declaration_status for units (
                select
                    (case
                        when units.max_declaration_finish_date is not null
                        then 'finished'
                        
                        when units.min_declaration_start_date is not null
                        then 'started'
                    end) as declaration_status
            )
        `);

        fs.writeFileSync(folderPath + "/c_order_declaration_status.sql", `
            cache c_order_declaration_status for orders (
                select
                    string_agg(distinct units.declaration_status, ', ') as declaration_status,
                    min( units.min_declaration_start_date ) as min_declaration_start_date,
                    max( units.max_declaration_finish_date ) as max_declaration_finish_date
                from units
                where
                    units.id_order = orders.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        
        await db.query(`
            insert into orders default values;
            insert into units (id_order) values (1);
            insert into declarations (id_unit, start_date) values (1, now());

            update declarations set
                finish_date = now()
        `);

        const actual = await db.query(`
            select declaration_status
            from orders
        `);
        assert.deepStrictEqual(actual.rows, [
            { declaration_status: "finished" }
        ]);
    });

    it("custom trigger dependent on self cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/test_two_cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                total_weight numeric
            );

            create table units (
                id serial primary key,
                id_order integer,
                box_weight numeric,
                quantity_boxes integer
            );
        `);

        fs.writeFileSync(folderPath + "/total_weight.sql", `
            cache total_weight for units (
                select
                    units.box_weight * 
                    coalesce(units.quantity_boxes, 1) as total_weight
            )
        `);

        fs.writeFileSync(folderPath + "/order_total_weight.sql", `
            create or replace function set_order_total()
            returns trigger as $body$
            begin
                update orders set
                    total_weight = (
                        select sum(units.total_weight)
                        from units
                        where units.id_order = orders.id
                    )
                where orders.id = new.id_order;

                return new;
            end
            $body$ language plpgsql;

            create trigger set_order_total
            after update of total_weight
            on units
            for each row
            execute procedure set_order_total()
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

        
        await db.query(`
            insert into orders default values;
            insert into units (id_order) values (1);

            update units set
                box_weight = 100.34;
        `);

        const actual = await db.query(`
            select total_weight
            from orders
        `);
        assert.deepStrictEqual(actual.rows, [
            { total_weight: "100.34" }
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
        
        // level 0, no deps
        fs.writeFileSync(folderPath + "/a.sql", `
            cache a for orders (
                select
                    orders.profit + 1 as profit_a
            )
        `);
    
        // level 1, dependent on a
        fs.writeFileSync(folderPath + "/c.sql", `
            cache c for orders (
                select
                    orders.profit_a + 10 as profit_c
            )
        `);

        // level 2, dependent on a and c
        fs.writeFileSync(folderPath + "/b.sql", `
            cache b for orders (
                select
                    orders.profit_c + 1000 + orders.profit_a as profit_b
            )
        `);
        // level 2, dependent on a and c
        fs.writeFileSync(folderPath + "/d.sql", `
            cache d for orders (
                select
                    orders.profit_c + 100 + orders.profit_a as profit_d
            )
        `);

        // level 3, dependent on b
        fs.writeFileSync(folderPath + "/e.sql", `
            cache e for orders (
                select
                    orders.profit_b + 100 as profit_e
            )
        `);
        // level 3, dependent on d
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

        let result = await db.query(`select * from orders`);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            profit: 10000,

            // level 0
            profit_a: 10001,
            // level 1
            profit_c: 10011,

            // level 2
            profit_b: 21012,
            profit_d: 20112,

            // level 3
            profit_e: 21112,
            profit_f: 20212
        }]);


        await db.query(`update orders set profit = 20000`);

        result = await db.query(`select * from orders`);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            profit: 20000,

            // level 0
            profit_a: 20001,
            // level 1
            profit_c: 20011,

            // level 2
            profit_b: 41012,
            profit_d: 40112,

            // level 3
            profit_e: 41112,
            profit_f: 40212
        }]);
    });

    it("one self cache with two columns and one column dependent on other", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                vat_value numeric,
                profit numeric(14, 2)
            );
        `);
        
        fs.writeFileSync(folderPath + "/self.sql", `
            cache self for orders (
                select
                    (orders.profit - orders.profit_vat)::numeric(14, 2) as profit_after_vat,
                    (orders.profit * orders.vat_value)::numeric(14, 2) as profit_vat
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into orders 
            (profit, vat_value) 
            values (1000, 0.20)
        `);

        let result = await db.query(`select * from orders`);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            vat_value: "0.20",
            profit: "1000.00",
            profit_vat: "200.00",
            profit_after_vat: "800.00"
        }]);


        await db.query(`update orders set vat_value = 0.16`);

        result = await db.query(`select * from orders`);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            vat_value: "0.16",
            profit: "1000.00",
            profit_vat: "160.00",
            profit_after_vat: "840.00"
        }]);
    });

    it("custom before trigger dependent on cache column", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                vat_value numeric,
                profit numeric(14, 2),
                profit_after_vat numeric(14, 2)
            );
        `);
        
        fs.writeFileSync(folderPath + "/self.sql", `
            cache self for orders (
                select
                    (orders.profit * orders.vat_value)::numeric(14, 2) as profit_vat
            )
        `);
        fs.writeFileSync(folderPath + "/custom.sql", `
            create or replace function z_before_trigger()
            returns trigger as $body$
            begin
                new.profit_after_vat = new.profit - new.profit_vat;
                return new;
            end
            $body$ language plpgsql;

            create trigger z_before_trigger
            before update of profit_vat
            on orders
            for each row
            execute procedure z_before_trigger();
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into orders 
            (profit, vat_value) 
            values (1000, 0);

            update orders set
                vat_value = 0.18
        `);

        let result = await db.query(`select * from orders`);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            vat_value: "0.18",
            profit: "1000.00",
            profit_vat: "180.00",
            profit_after_vat: "820.00"
        }]);
    });

    it("commutative dependent on one-row trigger", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );

            create table goods (
                id serial primary key,
                id_order integer,
                comment text,
                quantity integer,
                id_type integer
            );
            
            create table good_types (
                id serial primary key,
                weight numeric
            );

        `);
        
        fs.writeFileSync(folderPath + "/total_weight.sql", `
            cache a_total_weight for goods (
                select
                    good_types.weight * goods.quantity as total_weight
                from good_types
                where
                    good_types.id = goods.id_type
            )
        `);

        fs.writeFileSync(folderPath + "/order_goods.sql", `
            cache b_order_goods for orders (
                select
                    string_agg(distinct goods.comment, '; ') as comments,
                    sum( goods.total_weight ) as total_weight
                from goods
                where
                    goods.id_order = orders.id
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into orders default values;

            insert into good_types
                (weight)
            values
                (100),
                (200);

            insert into goods (id_order, quantity, id_type)
            values (1, 1, 1);

            update goods set
                comment = 'test-x',
                quantity = 10,
                id_type = 2;

            update goods set
                comment = 'test-y',
                quantity = 11,
                id_type = 1;
        `);

        let result = await db.query(`
            select id, comments, total_weight 
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            comments: "test-y",
            total_weight: "1100"
        }]);
    });

    it("one-row cache where one column dependent on other", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table goods (
                id serial primary key,
                quantity integer,
                id_type integer,
                bonus numeric default 1 not null
            );
            
            create table good_types (
                id serial primary key,
                price numeric(14, 2),
                bonus_quantity integer
            );
        `);
        
        fs.writeFileSync(folderPath + "/cost.sql", `
            cache cost for goods (
                select
                    (good_types.price * goods.quantity)::numeric(14, 2) as cost,

                    (case
                        when goods.quantity >= good_types.bonus_quantity
                        then goods.bonus * goods.cost
                        else goods.cost
                    end)::numeric(14, 2) as total_cost

                from good_types
                where
                    good_types.id = goods.id_type
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into good_types
                (price, bonus_quantity)
            values
                (100, 11),
                (200, 6);

            insert into goods (quantity, id_type, bonus)
            values (1, 1, 1);

            update goods set
                id_type = 2,
                bonus = 0.9,
                quantity = 7
        `);

        let result = await db.query(`
            select id, cost, total_cost
            from goods
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            cost: "1400.00",
            total_cost: "1260.00"
        }]);
    });

    it("custom trigger without dependency on cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table accounts (
                id serial primary key,
                vector smallint not null,
                balance numeric(14, 2)
            );
        `);

        fs.writeFileSync(folderPath + "/cache.sql", `
            cache has_50k for accounts (
                select (case
                    when accounts.vector = 1
                    then accounts.balance > 50000
                    else accounts.balance < 50000
                end) as has_50k
            )
        `);

        fs.writeFileSync(folderPath + "/validate_vector.sql", `
            create or replace function validate_vector()
            returns trigger as $body$
            begin
                raise exception 'vector is constant';
            end
            $body$ language plpgsql;

            create trigger validate_vector
            before update of vector
            on accounts
            for each row
            execute procedure validate_vector();
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into accounts
                (vector)
            values
                (1);

            update accounts set
                balance = 100
        `);
        assert.ok(true, "no errors");
    });

    it("commutative cache dependent on self cache, do update", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table accounts (
                id serial primary key,
                children_accounts_ids integer[]
            );
            create table operations (
                id serial primary key,
                id_account integer,
                sum numeric(14, 2)
            );
        `);

        fs.writeFileSync(folderPath + "/self.sql", `
            cache self for accounts (
                select array_append(
                    accounts.children_accounts_ids,
                    accounts.id
                ) as my_accounts_ids
            )
        `);

        fs.writeFileSync(folderPath + "/balance.sql", `
            cache balance for accounts (
                select sum( operations.sum )::numeric(14, 2) as balance
                from operations
                where
                    operations.id_account = any(accounts.my_accounts_ids)
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into accounts default values;
            insert into accounts default values;

            insert into operations
                (id_account, sum)
            values
                (1, 100),
                (2, 200);
            
            update accounts set
                children_accounts_ids = array[2]
            where id = 1
        `);

        let result = await db.query(`
            select id, balance
            from accounts
            where id = 1
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            balance: "300.00"
        }]);
    });

    it("commutative cache dependent on self cache on source table", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table accounts (
                id serial primary key
            );
            create table operations (
                id serial primary key,
                id_account integer,
                debit numeric(14, 2),
                credit numeric(14, 2)
            );
        `);

        fs.writeFileSync(folderPath + "/sum.sql", `
            cache sum for operations (
                select coalesce(
                    operations.debit,
                    -operations.credit
                )::numeric(14, 2) as sum
            )
        `);

        fs.writeFileSync(folderPath + "/balance.sql", `
            cache balance for accounts (
                select sum( operations.sum )::numeric(14, 2) as balance
                from operations
                where
                    operations.id_account = accounts.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into accounts default values;

            insert into operations
                (id_account, debit, credit)
            values
                (1, null, 100),
                (1, 200, null);
            
            update operations set
                debit = debit * 2,
                credit = credit * 2

        `);

        let result = await db.query(`
            select id, balance
            from accounts
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            balance: "200.00"
        }]);
    });

    it("commutative cache dependent on commutative cache with mutable target column", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );
            create table units (
                id serial primary key,
                id_order integer,
                container_weight float
            );
            create table goods (
                id serial primary key,
                id_unit integer,
                quantity integer,
                weight float
            );
        `);

        fs.writeFileSync(folderPath + "/goods.sql", `
            cache goods for units (
                select
                    units.container_weight + sum( goods.quantity * goods.weight )
                        as total_weight
                from goods
                where
                    goods.id_unit = units.id
            )
        `);

        fs.writeFileSync(folderPath + "/units.sql", `
            cache units for orders (
                select
                    sum( units.total_weight ) as total_weight
                from units
                where
                    units.id_order = orders.id
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into orders default values;
            insert into units (id_order, container_weight)
            values (1, 1000);

            insert into goods (id_unit, quantity, weight)
            values
                (1, 1000, 10),
                (1, 1500, 20);
            
            update units set
                container_weight = 1001
        `);

        let result = await db.query(`
            select id, total_weight
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            total_weight: String(
                1001 + 
                1000 * 10 + 
                1500 * 20
            )
        }]);
    });

    it("self cache with long name", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table this_is_long_name_of_test_table (
                id serial primary key,
                quantity integer,
                weight float
            );
        `);

        fs.writeFileSync(folderPath + "/long.sql", `
            cache this_is_long_name_of_test_cache
            for this_is_long_name_of_test_table (
                select
                    this_is_long_name_of_test_table.quantity * 
                    this_is_long_name_of_test_table.weight as total_weight
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into this_is_long_name_of_test_table
                (quantity, weight)
            values (2, 100);
        `);
        let result = await db.query(`
            select id, total_weight
            from this_is_long_name_of_test_table
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            total_weight: 200
        }]);

        await db.query(`
            update this_is_long_name_of_test_table set
                quantity = 10,
                weight = 300;
        `);
        result = await db.query(`
            select id, total_weight
            from this_is_long_name_of_test_table
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            total_weight: 3000
        }]);
    });

    it("custom trigger updating current row, test commutative trigger", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                container_type text
            );
            create table units (
                id serial primary key,
                id_order integer,
                container_type text,
                container_number text
            );
        `);

        fs.writeFileSync(folderPath + "/commutative.sql", `
            cache containers for orders (
                select
                    string_agg(distinct units.container_type, ', ') as units_containers_types,
                    string_agg(units.container_number, ', ') as units_containers_numbers
                from units
                where
                    units.id_order = orders.id
            )
        `);

        fs.writeFileSync(folderPath + "/custom.sql", `
            create or replace function custom()
            returns trigger as $body$
            begin
                update units set
                    container_type = (
                        select container_type
                        from orders
                        where orders.id = units.id_order
                    )
                where id = new.id;

                return new;
            end
            $body$ language plpgsql;

            create trigger a_custom
            after insert on units
            for each row
            execute procedure custom();
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into orders (container_type) values ('40HC');
            insert into units (id_order, container_number)
            values (1, 'ABCD123456'), (1, 'XYZC123456');
        `);
        let result = await db.query(`
            select
                id,
                units_containers_types,
                units_containers_numbers
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            units_containers_types: "40HC",
            units_containers_numbers: "ABCD123456, XYZC123456"
        }]);


        await db.query(`
            update units set
                container_number = 'test'
            where id = 2;
        `);
        result = await db.query(`
            select
                id,
                units_containers_types,
                units_containers_numbers
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            units_containers_types: "40HC",
            units_containers_numbers: "ABCD123456, test"
        }]);
    });

    it("table name is keyword", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table public.order (
                id serial primary key,
                id_prev_order integer,
                name text
            );
        `);

        fs.writeFileSync(folderPath + "/prev_order.sql", `
            cache prev_order for public.order as next_order (
                select
                    string_agg(public.order.name, ', ') as prev_name
                from public.order
                where
                    next_order.id_prev_order = public.order.id
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            insert into public.order (id_prev_order, name)
            values (null, 'prev'), (1, 'next')
        `);
        const result = await db.query(`
            select id, name, prev_name
            from public.order
            order by id
        `);

        assert.deepStrictEqual(result.rows, [
            {id: 1, name: "prev", prev_name: null},
            {id: 2, name: "next", prev_name: "prev"}
        ]);
    });

    it("using 101 column in commutative cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        const columns: string[] = [];
        for (let i = 1; i <= 101; i++) {
            columns.push(`value${i}`);
        }

        await db.query(`
            create table public.order (
                id serial primary key,
                id_parent_order integer,
                ${columns.map(column => 
                    `${column} integer`
                ).join(
                    ", "
                )}
            );
        `);

        fs.writeFileSync(folderPath + "/children.sql", `
            cache children for public.order as parent (
                select
                    ${columns.map(column => 
                        `sum( child.${column} ) as child_${column}`
                    ).join(
                        ", "
                    )}
                from public.order as child
                where
                    child.id_parent_order = parent.id
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            insert into public.order default values;
            insert into public.order (id_parent_order, ${columns})
            values
                (1, ${columns.map((column, i) => i + 1)})
        `);
        const result = await db.query(`
            select *
            from public.order
            where id = 1
        `);

        const parentOrder = result.rows[0];
        for (let i = 0; i < columns.length; i++) {
            const column = columns[i];
            const cacheColumn = `child_${column}`;

            assert.strictEqual(
                +parentOrder[cacheColumn],
                i + 1,
                cacheColumn
            );
        }
    });

    it("correct brackets", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table public.order (
                id serial primary key,
                a integer,
                b integer,
                c integer,
                d integer
            );
        `);

        fs.writeFileSync(folderPath + "/x.sql", `
            cache x for public.order as item (
                select
                    (item.a + item.b) / (item.c + item.d) as x
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        await db.query(`
            insert into public.order
                (a,b,c,d)
            values
                (50, 150, 4, 6)
        `);
        const result = await db.query(`
            select x
            from public.order
            where id = 1
        `);

        assert.deepStrictEqual(
            result.rows[0],
            {x: 20}
        );
    });

    it("cache trigger dependent on custom before trigger", async() => {
        const folderPath = ROOT_TMP_PATH + "/sort-deps";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key,
                a integer,
                b integer
            );
        `);
        
        fs.writeFileSync(folderPath + "/self.sql", `
            cache self for orders (
                select
                    orders.b * 10 as c
            )
        `);
        fs.writeFileSync(folderPath + "/custom.sql", `
            create or replace function before_trigger()
            returns trigger as $body$
            begin
                new.b = new.a * 2;

                return new;
            end
            $body$ language plpgsql;

            create trigger before_trigger
            before insert or update of a
            on orders
            for each row
            execute procedure before_trigger();
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into orders (a)
            values (100);
        `);

        let result = await db.query(`select a, b, c from orders`);
        assert.deepStrictEqual(result.rows, [{
            a: 100,
            b: 200,
            c: 2000
        }]);


        await db.query(`
            update orders set
                a = 10
        `);

        result = await db.query(`select a, b, c from orders`);
        assert.deepStrictEqual(result.rows, [{
            a: 10,
            b: 20,
            c: 200
        }]);
    });

    it("last row sort by agg cache column and filter by array", async() => {
        // 11595
        const folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table declarations (
                id serial primary key,
                units_ids integer[]
            );

            create table operations (
                id serial primary key,
                units_ids integer[]
            );

            create table points (
                id serial primary key,
                id_operation integer,
                actual_date text
            );
        `);

        fs.writeFileSync(folderPath + "/max_date.sql", `
            cache max_date for operations (
                select
                    max( points.actual_date ) as max_date
                from points
                where
                    points.id_operation = operations.id
            )
        `);
        fs.writeFileSync(folderPath + "/last_operation.sql", `
            cache last_operation for declarations (
                select 
                last_operation.id as last_operation_id
                from operations as last_operation
                where
                    last_operation.units_ids && declarations.units_ids

                order by 
                    last_operation.max_date desc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into declarations (units_ids)
            values (array[2, 4]);

            insert into operations (units_ids)
            values (array[1, 2]);

            insert into points (id_operation, actual_date)
            values
                (1, '22.10.2022'),
                (1, '23.10.2022');
        `);
        let result = await db.query(`
            select id, last_operation_id
            from declarations
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            last_operation_id: 1
        }], "created first operation");


        await db.query(`
            insert into operations (units_ids)
            values (array[2, 3]);

            insert into points (id_operation, actual_date)
            values
                (2, '21.10.2022'),
                (2, '22.10.2022');
        `);
        result = await db.query(`
            select id, last_operation_id
            from declarations
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            last_operation_id: 1
        }], "created second operation");


        await db.query(`
            update points set
                actual_date = '24.10.2022'
            where id_operation = 2 and id = 3;
        `);
        result = await db.query(`
            select id, last_operation_id
            from declarations
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            last_operation_id: 2
        }], "update first point in second operation");
    });

    it("last row sort by agg cache column and filter by mutable column", async() => {
        // 11595
        const folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table declarations (
                id serial primary key,
                id_order integer
            );

            create table operations (
                id serial primary key,
                id_order integer
            );

            create table points (
                id serial primary key,
                id_operation integer,
                actual_date text
            );
        `);

        fs.writeFileSync(folderPath + "/max_date.sql", `
            cache max_date for operations (
                select
                    max( points.actual_date ) as max_date
                from points
                where
                    points.id_operation = operations.id
            )
        `);
        fs.writeFileSync(folderPath + "/last_operation.sql", `
            cache last_operation for declarations (
                select 
                last_operation.id as last_operation_id
                from operations as last_operation
                where
                    last_operation.id_order = declarations.id_order

                order by 
                    last_operation.max_date desc
                limit 1
            )
        `);

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            insert into declarations (id_order)
            values (101);

            insert into operations (id_order)
            values (101);

            insert into points (id_operation, actual_date)
            values
                (1, '22.10.2022'),
                (1, '23.10.2022');
        `);
        let result = await db.query(`
            select id, last_operation_id
            from declarations
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            last_operation_id: 1
        }], "created first operation");


        await db.query(`
            insert into operations (id_order)
            values (101);

            insert into points (id_operation, actual_date)
            values
                (2, '21.10.2022'),
                (2, '22.10.2022');
        `);
        result = await db.query(`
            select id, last_operation_id
            from declarations
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            last_operation_id: 1
        }], "created second operation");


        await db.query(`
            update points set
                actual_date = '24.10.2022'
            where id_operation = 2 and id = 3;
        `);
        result = await db.query(`
            select id, last_operation_id
            from declarations
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            last_operation_id: 2
        }], "update first point in second operation");
    });

    it("last row sort by agg cache column and filter by two cache columns", async() => {
        // 11595
        const folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );

            create table operations (
                id serial primary key,
                id_order integer
            );

            create table points (
                id serial primary key,
                id_operation integer,
                sort integer,
                actual_date text
            );
        `);

        fs.writeFileSync(folderPath + "/max_date.sql", `
            cache max_date for operations (
                select
                    max( points.actual_date ) as max_date
                from points
                where
                    points.id_operation = operations.id
            )
        `);
        fs.writeFileSync(folderPath + "/end_date.sql", `
            cache end_date for operations (
                select
                    points.actual_date as end_date
                from points
                where
                    points.id_operation = operations.id

                order by points.sort desc
                limit 1
            )
        `);
        fs.writeFileSync(folderPath + "/end_operation_date.sql", `
            cache end_operation_date for orders (
                select 
                    last_operation.end_date as end_operation_date
                from operations as last_operation
                where
                    last_operation.id_order = orders.id and
                    last_operation.max_date is not null and
                    last_operation.end_date is not null

                order by 
                    last_operation.max_date desc
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

            insert into operations (id_order)
            values (1);

            insert into points (id_operation, sort, actual_date)
            values
                (1, 1, '22.10.2022'),
                (1, 2, '23.10.2022');
        `);
        let result = await db.query(`
            select id, end_operation_date
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            end_operation_date: "23.10.2022"
        }], "created operation");


        await db.query(`
            update points set
                actual_date = '24.10.2022',
                sort = 3
            where sort = 1;
        `);
        result = await db.query(`
            select id, end_operation_date
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            end_operation_date: "24.10.2022"
        }], "update first point in operation");
    });

    it("last row sort by agg cache column and filter by cache columns, also update current row by custom trigger", async() => {
        // 11595
        const folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );

            create table operations (
                id serial primary key,
                id_order integer
            );

            create table points (
                id serial primary key,
                id_operation integer,
                sort integer,
                actual_date text,
                is_last boolean
            );
        `);

        fs.writeFileSync(folderPath + "/some_custom_trigger.sql", `
            create or replace function set_is_last()
            returns trigger as $body$
            begin

                update points as current_point set
                    is_last = not exists(
                        select from points as next_point
                        where
                            next_point.id_operation = current_point.id_operation and
                            next_point.sort > current_point.sort
                    )
                where id = new.id;

                return new;
            end
            $body$ language plpgsql;

            create trigger a_set_is_last
            after insert or update of id_operation, sort
            on points
            for each row
            execute procedure set_is_last();
        `);

        fs.writeFileSync(folderPath + "/max_date.sql", `
            cache max_date for operations (
                select
                    max( points.actual_date ) as max_date
                from points
                where
                    points.id_operation = operations.id
            )
        `);
        fs.writeFileSync(folderPath + "/end_date.sql", `
            cache end_date for operations (
                select
                    points.actual_date as end_date
                from points
                where
                    points.id_operation = operations.id

                order by points.sort desc
                limit 1
            )
        `);
        fs.writeFileSync(folderPath + "/end_operation_date.sql", `
            cache end_operation_date for orders (
                select 
                    last_operation.end_date as end_operation_date
                from operations as last_operation
                where
                    last_operation.id_order = orders.id and
                    last_operation.max_date is not null and
                    last_operation.end_date is not null

                order by 
                    last_operation.max_date desc
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

            insert into operations (id_order)
            values (1);

            insert into points (id_operation, sort, actual_date)
            values
                (1, 1, '22.10.2022'),
                (1, 2, '23.10.2022');
        `);
        let result = await db.query(`
            select id, end_operation_date
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            end_operation_date: "23.10.2022"
        }], "created operation");


        await db.query(`
            update points set
                actual_date = '24.10.2022',
                sort = 3
            where sort = 1;
        `);
        result = await db.query(`
            select id, end_operation_date
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            end_operation_date: "24.10.2022"
        }], "update first point in operation");
    });

    it("last row sort by agg cache column and filter by cache column and custom trigger column", async() => {
        // 11595
        const folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );

            create table operations (
                id serial primary key,
                id_order integer
            );

            create table points (
                id serial primary key,
                id_operation integer,
                sort integer,
                actual_date text,
                is_last boolean
            );
        `);

        fs.writeFileSync(folderPath + "/some_custom_trigger.sql", `
            create or replace function set_is_last()
            returns trigger as $body$
            begin

                update points as current_point set
                    is_last = not exists(
                        select from points as next_point
                        where
                            next_point.id_operation = current_point.id_operation and
                            next_point.sort > current_point.sort
                    )
                where id = new.id;

                return new;
            end
            $body$ language plpgsql;

            create trigger set_is_last
            after insert or update of id_operation, sort
            on points
            for each row
            execute procedure set_is_last();
        `);
        fs.writeFileSync(folderPath + "/max_date.sql", `
            cache max_date for operations (
                select
                    max( points.actual_date ) as max_date
                from points
                where
                    points.id_operation = operations.id
            )
        `);
        fs.writeFileSync(folderPath + "/last_date.sql", `
            cache last_date for operations (
                select
                    points.actual_date as last_date
                from points
                where
                    points.id_operation = operations.id and
                    points.is_last

                order by points.id desc
                limit 1
            )
        `);

        fs.writeFileSync(folderPath + "/max_operation_date.sql", `
            cache end_operation_date for orders (
                select 
                    last_operation.max_date as max_operation_date
                from operations as last_operation
                where
                    last_operation.id_order = orders.id and
                    last_operation.max_date is not null and
                    last_operation.last_date is not null

                order by 
                    last_operation.max_date desc
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

            insert into operations (id_order)
            values (1);

            insert into points (id_operation, sort, actual_date)
            values
                (1, 1, '22.10.2022'),
                (1, 2, '23.10.2022');
        `);
        let result = await db.query(`
            select id, max_operation_date
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            max_operation_date: "23.10.2022"
        }], "created operation");


        await db.query(`
            update points set
                actual_date = '24.10.2022',
                sort = 3
            where sort = 1;
        `);
        result = await db.query(`
            select id, max_operation_date
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            id: 1,
            max_operation_date: "24.10.2022"
        }], "update first point in operation");
    });

    it("one last row by id, check special conditions on insert", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );

            create table operations (
                id serial primary key,
                id_order integer,
                doc_number text,
                sub_type text,
                deleted smallint default 0 not null
            );
        `);

        fs.writeFileSync(folderPath + "/last_ocean_oper.sql", `
            cache last_ocea_oper for orders (
                select
                    operations.doc_number as last_ocean_oper_number
                from operations
                where
                    operations.id_order = orders.id and
                    operations.sub_type = 'ocean' and
                    operations.deleted = 0

                order by operations.id desc
                limit 1
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            -- order1
            insert into orders default values;

            insert into operations (id_order)
            values (1), (1);

            update operations set
                doc_number = 'a',
                sub_type = 'ocean'
            where id = 1;

            update operations set
                doc_number = 'b',
                sub_type = 'ocean'
            where id = 2;


            update operations set
                deleted = 1
            where id = 2;
        `);
        const result = await db.query(`
            select last_ocean_oper_number
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            last_ocean_oper_number: "a"
        }]);
    });

    it("one last row by mutable column, check special conditions on insert", async() => {
        const folderPath = ROOT_TMP_PATH + "/cache";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table orders (
                id serial primary key
            );

            create table operations (
                id serial primary key,
                id_order integer,
                doc_number text,
                sub_type text,
                deleted smallint default 0 not null,
                level integer default 1
            );
        `);

        fs.writeFileSync(folderPath + "/last_ocean_oper.sql", `
            cache last_ocea_oper for orders (
                select
                    operations.doc_number as last_ocean_oper_number
                from operations
                where
                    operations.id_order = orders.id and
                    operations.sub_type = 'ocean' and
                    operations.deleted = 0

                order by operations.level desc
                limit 1
            )
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });


        await db.query(`
            -- order1
            insert into orders default values;

            insert into operations (id_order, level)
            values (1, 1), (1, 2);

            update operations set
                doc_number = 'a',
                sub_type = 'ocean'
            where id = 1;

            update operations set
                doc_number = 'b',
                sub_type = 'ocean'
            where id = 2;


            update operations set
                deleted = 1
            where id = 2;
        `);
        const result = await db.query(`
            select last_ocean_oper_number
            from orders
        `);
        assert.deepStrictEqual(result.rows, [{
            last_ocean_oper_number: "a"
        }]);
    });

    it("test cache with (and) or (and) with mistake", async() => {
        await db.query(`
            create table invoices (
                id serial primary key,
                invoice_type text,
                id_buyer integer,
                id_seller integer
            );
            create table companies (
                id serial primary key,
                name text,
                deleted integer default 0
            );
        `);

        fs.writeFileSync(ROOT_TMP_PATH + "/our_company.sql", `
            cache our_company for invoices (
                select
                    companies.name as our_company_name
                from companies
                where
                    (
                        invoices.invoice_type = 'incoming' and
                        companies.id = invoices.id_buyer
                    )
                    or
                    (
                        invoices.invoice_type = 'outgoing' and
                        companies.id = invoices.id_seller
                    )
                    -- mistake
                    and companies.deleted = 0 
            )
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });


        await db.query(`
            insert into companies (name) values ('our company');

            insert into invoices
                (invoice_type, id_buyer, id_seller)
            values
                ('incoming', 1, null),
                ('outgoing', null, 1)
        `);
        const result = await db.query(`
            select
                id, our_company_name
            from invoices
            order by id
        `);

        assert.deepStrictEqual(result.rows, [{
            id: 1,
            our_company_name: "our company"
        }, {
            id: 2,
            our_company_name: "our company"
        }]);
    });

    it("test cache with (and) or (and) without mistake", async() => {
        await db.query(`
            create table invoices (
                id serial primary key,
                invoice_type text,
                id_buyer integer,
                id_seller integer
            );
            create table companies (
                id serial primary key,
                name text,
                deleted integer default 0
            );
        `);

        fs.writeFileSync(ROOT_TMP_PATH + "/our_company.sql", `
            cache our_company for invoices (
                select
                    companies.name as our_company_name
                from companies
                where
                    (
                        invoices.invoice_type = 'incoming' and
                        companies.id = invoices.id_buyer
                        or
                        invoices.invoice_type = 'outgoing' and
                        companies.id = invoices.id_seller
                    )
                    -- mistake
                    and companies.deleted = 0 
            )
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: true
        });

        // create invoices and company
        await db.query(`
            insert into companies (name) values ('our company');

            insert into invoices
                (invoice_type, id_buyer, id_seller)
            values
                ('incoming', 1, null),
                ('outgoing', null, 1);
            
        `);
        const result1 = await db.query(`
            select
                id, our_company_name
            from invoices
            order by id
        `);
        assert.deepStrictEqual(result1.rows, [{
            id: 1,
            our_company_name: "our company"
        }, {
            id: 2,
            our_company_name: "our company"
        }], "company.deleted = 0");

        // trash company
        await db.query(`
            update companies set
                deleted = 1
        `);
        const result2 = await db.query(`
            select
                id, our_company_name
            from invoices
            order by id
        `);

        assert.deepStrictEqual(result2.rows, [{
            id: 1,
            our_company_name: null
        }, {
            id: 2,
            our_company_name: null
        }], "company.deleted = 1");
    });

    // TODO: update-ddl-cache in watcher mode

    async function sleep(ms: number) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

});