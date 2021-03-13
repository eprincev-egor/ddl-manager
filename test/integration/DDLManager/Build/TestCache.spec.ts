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
});