import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../../getDbClient";
import { DDLManager } from "../../../../lib/DDLManager";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { PostgresDriver } from "../../../../lib/database/PostgresDriver";

use(chaiShallowDeepEqualPlugin);

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/DDLManager.build functions", () => {
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
    
    it("build simple function", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        const rnd = Math.round( 1000 * Math.random() );
        fs.writeFileSync(folderPath + "/nice.sql", `
            create or replace function nice(a integer)
            returns integer as $body$
                begin
                    return a * ${ rnd };
                end
            $body$
            language plpgsql;
        `);


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        const result = await db.query("select nice(2) as nice");
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            nice: 2 * rnd
        });
    });

    it("replace function", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-func";
        let result;
        let row;
    
        fs.mkdirSync(folderPath);

        const rnd = Math.round( 1000 * Math.random() );
        fs.writeFileSync(folderPath + "/nice.sql", `
            create or replace function nice(a integer)
            returns integer as $body$
                begin
                    return a * ${ rnd };
                end
            $body$
            language plpgsql;
        `);


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("select nice(2) as nice");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            nice: 2 * rnd
        });

        fs.writeFileSync(folderPath + "/nice.sql", `
            create or replace function nice()
            returns integer as $body$
                begin
                    return 100;
                end
            $body$
            language plpgsql;
        `);

        await DDLManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("select nice() as nice");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            nice: 100
        });

        // old function must be dropped
        try {
            await db.query("select nice(2) as nice");
            assert.ok(false, "expected error");
        } catch(err) {
            assert.equal(err.message, "function nice(integer) does not exist");
        }
    });

    it("remove function", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        fs.writeFileSync(folderPath + "/nice.sql", `
            create or replace function nice()
            returns integer as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;
        `);


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        const result = await db.query("select nice() as nice");
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            nice: 1
        });


        // remove file for remove function from db
        fs.unlinkSync(folderPath + "/nice.sql");

        await DDLManager.build({
            db, 
            folder: folderPath
        });

        try {
            await db.query("select nice() as nice");
            throw new Error("expected error");
        } catch(err) {
            assert.equal(err.message, "function nice() does not exist");
        }
    });


    it("build two functions, one syntax error, one success", async() => {
        fs.writeFileSync(ROOT_TMP_PATH + "/func1.sql", `
            create or replace function func1(a integer)
            returns integer as $body$
                begin
                    return 1;
                end
            $body$
            language ERROR;
        `);

        fs.writeFileSync(ROOT_TMP_PATH + "/func2.sql", `
            create or replace function func2(a integer)
            returns integer as $body$
                begin
                    return 2;
                end
            $body$
            language plpgsql;
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH
        });

        const result = await db.query("select func2(1) as func2");
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            func2: 2
        });

        try {
            await db.query("select func1(1)");
            throw new Error("expected error");
        } catch(err) {
            assert.equal(err.message, "function func1(integer) does not exist");
        }
    });


    it("build two functions, one frozen", async() => {
        // create frozen function
        await db.query(`
            create or replace function func1(a integer)
            returns integer as $body$
                begin
                    return 0;
                end
            $body$
            language plpgsql;
        `);

        fs.writeFileSync(ROOT_TMP_PATH + "/func1.sql", `
            create or replace function func1(a integer)
            returns integer as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;
        `);

        fs.writeFileSync(ROOT_TMP_PATH + "/func2.sql", `
            create or replace function func2(a integer)
            returns integer as $body$
                begin
                    return 2;
                end
            $body$
            language plpgsql;
        `);


        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: false
        });

        let result;

        result = await db.query("select func2(1) as func2");

        expect(result.rows[0]).to.be.shallowDeepEqual({
            func2: 2
        });

        result = await db.query("select func1(1) as func1");

        expect(result.rows[0]).to.be.shallowDeepEqual({
            func1: 1
        });
    });

    it("build simple function with comment", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        fs.writeFileSync(folderPath + "/nice.sql", `
            create or replace function nice(a integer)
            returns integer as $body$
                begin
                    return a * 1;
                end
            $body$
            language plpgsql;

            comment on function nice(integer) is $$good$$;
        `);


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        const result = await db.query(`
            select
                pg_catalog.obj_description( pg_proc.oid ) as comment
            from information_schema.routines as routines

            left join pg_catalog.pg_proc as pg_proc on
                routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text
        
            where
                routines.routine_schema = 'public' and
                routines.routine_name = 'nice'
        `);

        expect(result.rows[0]).to.be.shallowDeepEqual({
            comment: "good\nddl-manager-sync"
        });
    });

    it("drop function with comment, after dump", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-trigger";
        fs.mkdirSync(folderPath);

        await db.query(`
            create or replace function test()
            returns bigint as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;

            comment on function test() is 'test';
        `);

        let result;
        let row;

        result = await db.query("select test() as test");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            test: 1
        });

        await DDLManager.dump({
            db,
            folder: folderPath,
            unfreeze: true
        });

        // remove file with trigger
        fs.unlinkSync(folderPath + "/public/test.sql");
        
        // drop trigger by build command
        await DDLManager.build({
            db, 
            folder: folderPath
        });

        try {
            await db.query("select test()");
            assert.ok(false, "expected error");
        } catch(err) {
            assert.equal(err.message, "function test() does not exist");
        }
    });

    it("build function with comment, after dump, comment must be exists", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-trigger";
        fs.mkdirSync(folderPath);

        await db.query(`
            create or replace function test()
            returns bigint as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;

            comment on function test() is 'test';
        `);

        let result;
        let row;

        result = await db.query("select test() as test");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            test: 1
        });

        await DDLManager.dump({
            db,
            folder: folderPath,
            unfreeze: true
        });

        await DDLManager.build({
            db, 
            folder: folderPath
        });

        // comment must be exists
        result = await db.query(`
            select
                coalesce(
                    pg_catalog.obj_description( pg_proc.oid ),
                    'dropped'
                ) as comment
            from information_schema.routines as routines

            left join pg_catalog.pg_proc as pg_proc on
                routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text
        
            where
                routines.routine_schema = 'public' and
                routines.routine_name = 'test'
        `);

        expect(result.rows[0]).to.be.shallowDeepEqual({
            comment: "test\nddl-manager-sync"
        });
    });

    it("build when function has change, but trigger not", async() => {
        let result;
        let row;

        const folderPath = ROOT_TMP_PATH + "/simple-trigger";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table company (
                name text primary key,
                note text
            );
        `);
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            create or replace function set_note_before_insert_or_update_name()
            returns trigger as $body$
                begin
                    new.note = 'name: ' || new.name;
                    return new;
                end
            $body$
            language plpgsql;

            create trigger set_note_before_insert_or_update_name_trigger
            before insert or update of name
            on company
            for each row
            execute procedure set_note_before_insert_or_update_name();
        `);


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("insert into company (name) values ('super') returning note");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            note: "name: super"
        });

        // change only function
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            create or replace function set_note_before_insert_or_update_name()
            returns trigger as $body$
                begin
                    new.note = 'changed: ' || new.name;
                    return new;
                end
            $body$
            language plpgsql;

            create trigger set_note_before_insert_or_update_name_trigger
            before insert or update of name
            on company
            for each row
            execute procedure set_note_before_insert_or_update_name();
        `);

        await DDLManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("insert into company (name) values ('super 2') returning note");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            note: "changed: super 2"
        });
    });


    
    it("build function(returns record), after dump", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        const body1 = `
            begin
                x = 10;
            end
        `;
        const body2 = `
            begin
                z = a;
            end
        `;

        await db.query(`
            create or replace function test(out x integer, out y text)
            returns record as $body$${ body1 }$body$
            language plpgsql;
        `);

        await DDLManager.dump({
            db,
            folder: folderPath,
            unfreeze: true
        });

        // add new function to file
        let fileContent = fs.readFileSync( folderPath + "/public/test.sql" ).toString();
        fileContent += `

create or replace function test(a integer, out z integer, out y text)
returns record as $body$${ body2 }$body$
language plpgsql;
        `;
        
        fs.writeFileSync( folderPath + "/public/test.sql", fileContent );


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        const result = await db.query(`
            select
                f1.x + f2.z as total
            from test() as f1, test(30) as f2
        `);
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            total: 40
        });
    });

    it("build two functions from one file, using two separators ';'", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        const body1 = `
            begin
                return 1;
            end
        `;
        const body2 = `
            begin
                return x;
            end
        `;

        fs.writeFileSync( folderPath + "/test.sql", `
create or replace function test()
returns bigint as $$${ body1 }$$
language plpgsql;

;
;
;

create or replace function test(x integer)
returns bigint as $$${ body2 }$$
language plpgsql;
        `);


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        const result = await db.query(`
            select
                f1 + f2 as total
            from test() as f1, test(30) as f2
        `);
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            total: 31
        });
    });

    it("build function with type public.\"order\" inside arguments", async() => {
        const folderPath = ROOT_TMP_PATH + "/quotes-arg-func";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table public.order (
                id serial primary key,
                name text not null unique
            );
        `);
        
        fs.writeFileSync(folderPath + "/zip_order.sql", `
            create or replace function zip_order(
                order_row public.order
            )
            returns text as $body$
                begin
                    return order_row.id || ',' || order_row.name;
                end
            $body$
            language plpgsql;
        `);


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        let result;
        let row;
        
        result = await db.query(`
            insert into public.order 
                (name) 
            values 
                ('test')
            returning *
        `);
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            id: 1,
            name: "test"
        });

        // test function
        result = await db.query(`
            select 
                zip_order( order_row ) as zip 
            from public.order as order_row
            where 
                id = 1
        `);
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            zip: "1,test"
        });

        // change function code
        fs.writeFileSync(folderPath + "/zip_order.sql", `
            create or replace function zip_order(
                order_row public.order
            )
            returns text as $body$
                begin
                    return order_row.id || ':' || order_row.name;
                end
            $body$
            language plpgsql;
        `);


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        // and again test function
        result = await db.query(`
            select 
                zip_order( order_row ) as zip 
            from public.order as order_row
            where 
                id = 1
        `);
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            zip: "1:test"
        });
    });

    it("ignore errors like are 'cannot drop ... because other objects depend on it' on drop functon", async() => {
        await db.query(`
            create or replace function my_func()
            returns text as $body$
            begin
                return 'test';
            end
            $body$
            language plpgsql;

            create view my_view as
                select my_func() as my_func;
        `);

        const folderPath = ROOT_TMP_PATH + "/ignore-cascades";
        fs.mkdirSync(folderPath);

        await DDLManager.dump({
            db,
            folder: folderPath,
            unfreeze: true
        });

        let result = await db.query(`
            select *
            from my_view
        `);
        let row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            my_func: "test"
        });

        fs.writeFileSync(folderPath + "/public/my_func.sql", `
            create or replace function my_func()
            returns text as $body$
            begin
                return 'nice';
            end
            $body$
            language plpgsql;
        `);

        await DDLManager.build({
            db, 
            folder: folderPath
        });


        result = await db.query(`
            select *
            from my_view
        `);
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            my_func: "nice"
        });
    });

    it("recreate freezed function with another argument name", async() => {
        await db.query(`
            create or replace function my_int_func(x integer)
            returns text as $body$
            begin
                return 'test ' || x;
            end
            $body$
            language plpgsql;
        `);

        const folderPath = ROOT_TMP_PATH + "/test-inf8";
        fs.mkdirSync(folderPath);

        let result = await db.query(`
            select my_int_func(101) as my_int_func
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            my_int_func: "test 101"
        });

        fs.writeFileSync(folderPath + "/my_int_func.sql", `
            create or replace function my_int_func(y integer)
            returns text as $body$
            begin
                return 'nice ' || y;
            end
            $body$
            language plpgsql;
        `);

        await DDLManager.build({
            db, 
            folder: folderPath
        });


        result = await db.query(`
            select my_int_func(101) as my_int_func
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            my_int_func: "nice 101"
        });
    });

    it("change function and frozen status on build", async() => {
        // create frozen function
        await db.query(`
            create or replace function func1(a integer)
            returns integer as $body$
                begin
                    return 0;
                end
            $body$
            language plpgsql;
        `);

        fs.writeFileSync(ROOT_TMP_PATH + "/func1.sql", `
            create or replace function func1(a integer)
            returns integer as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;
        `);

        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: false
        });

        const result = await db.query("select func1(1) as func1");
        expect(result.rows[0]).to.be.shallowDeepEqual({
            func1: 1
        });

        const postgres = new PostgresDriver(db);
        const funcs = (await postgres.load()).functions;

        expect(funcs[0]).to.be.shallowDeepEqual({
            name: "func1",
            frozen: false
        });
    });

    it("don't drop frozen functions", async() => {
        await db.query(`
            create or replace function my_func()
            returns text as $body$
            begin
                return 'test';
            end
            $body$
            language plpgsql;

            create view my_view as
                select my_func() as my_func;
        `);

        const folderPath = ROOT_TMP_PATH + "/some-frozen-func";
        fs.mkdirSync(folderPath);

        await DDLManager.dump({
            db,
            folder: folderPath
        });

        let result = await db.query(`
            select *
            from my_view
        `);
        let row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            my_func: "test"
        });

        fs.unlinkSync(folderPath + "/public/my_func.sql");

        await DDLManager.build({
            db, 
            folder: folderPath
        });


        result = await db.query(`
            select *
            from my_view
        `);
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            my_func: "test"
        });
    });

    it("build function when exists view with call that function", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);


        await db.query(`
            create or replace function nice()
            returns integer as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;

            create view test_view as
                select nice() as numb
        `);

        fs.writeFileSync(folderPath + "/nice.sql", `
            create or replace function nice()
            returns integer as $body$
                begin
                    return 2;
                end
            $body$
            language plpgsql;
        `);


        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        const result = await db.query("select numb from test_view");
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            numb: 2
        });
    });
});