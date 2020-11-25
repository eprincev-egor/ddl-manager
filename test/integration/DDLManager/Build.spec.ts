import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../getDbClient";
import { DDLManager } from "../../../lib/DDLManager";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";

use(chaiShallowDeepEqualPlugin);

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/DDLManager.build", () => {
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
    
    it("build nonexistent folder", async() => {
        try {
            await DDLManager.build({
                db, 
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("build empty folder", async() => {
        const folderPath = ROOT_TMP_PATH + "/empty";
        fs.mkdirSync(folderPath);

        await DDLManager.build({
            db, 
            folder: folderPath
        });

        // expected build without errors
        assert.ok(true);
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

    it("build with dbConfig", async() => {
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
            db: {
                database: db.database,
                user: db.user,
                password: db.password,
                host: db.host,
                port: db.port
            }, 
            folder: folderPath
        });

        const result = await db.query("select nice() as nice");
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            nice: 1
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

    it("build simple trigger", async() => {
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

        const result = await db.query("insert into company (name) values ('super') returning note");
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            note: "name: super"
        });
    });

    it("replace trigger", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-trigger";
        let result;
        let row;
    
        fs.mkdirSync(folderPath);

        await db.query(`
            create table company (
                name text primary key,
                note text
            );
        `);
        
        // first content
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

        
        // second content
        
        fs.writeFileSync(folderPath + "/set_note_trigger.sql", `
            create or replace function set_note_before_insert_or_update_name()
            returns trigger as $body$
                begin
                    new.note = 'nice: ' || new.name;
                    return new;
                end
            $body$
            language plpgsql;

            create trigger set_note_before_insert_or_update_name_trigger
            before insert or update
            on company
            for each row
            execute procedure set_note_before_insert_or_update_name();
        `);


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("insert into company (name) values ('test') returning note");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            note: "nice: test"
        });
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


    it("remove trigger", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table company (
                name text,
                note text
            );
        `);
        
        // first content
        fs.writeFileSync(folderPath + "/some_trigger.sql", `
            create or replace function raise_error_on_some()
            returns trigger as $body$
                begin
                    raise exception 'success';
                end
            $body$
            language plpgsql;

            create trigger raise_error_on_some_trigger
            after insert or update or delete
            on company
            for each row
            execute procedure raise_error_on_some();
        `);


        await DDLManager.build({
            db, 
            folder: folderPath
        });

        try {
            await db.query("insert into company default values");
            throw new Error("expected error");
        } catch(err) {
            assert.equal(err.message, "success");
        }


        // remove file for remove trigger from db
        fs.unlinkSync(folderPath + "/some_trigger.sql");

        await DDLManager.build({
            db, 
            folder: folderPath
        });

        try {
            await db.query("insert into company default values");
        } catch(err) {
            assert.ok(false, "unexpected error: " + err.message);
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

    it("drop trigger with comment, after dump", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-trigger";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table company (
                name text primary key,
                note text
            );

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

            comment on trigger set_note_before_insert_or_update_name_trigger
            on company is 'test';
        `);

        let result;
        let row;

        result = await db.query("insert into company (name) values ('super') returning note");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            note: "name: super"
        });

        await DDLManager.dump({
            db,
            folder: folderPath,
            unfreeze: true
        });

        // remove file with trigger
        fs.unlinkSync(folderPath + "/public/company/set_note_before_insert_or_update_name.sql");
        
        // drop trigger by build command
        await DDLManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("insert into company (name) values ('super 2') returning note");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            note: null
        });
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

    it("build trigger with comment, after dump, comment must be exists", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-trigger";
        fs.mkdirSync(folderPath);

        await db.query(`
            create table company (
                name text primary key,
                note text
            );

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

            comment on trigger set_note_before_insert_or_update_name_trigger
            on company is 'test';
        `);

        let result;
        let row;

        result = await db.query("insert into company (name) values ('super') returning note");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            note: "name: super"
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
                    pg_catalog.obj_description( pg_trigger.oid ),
                    'dropped'
                ) as comment
            from pg_trigger
            where
                pg_trigger.tgname = 'set_note_before_insert_or_update_name_trigger'
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

    it("don't drop frozen functions and triggers", async() => {
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

    it("build from many folders", async() => {

        const folderPath1 = ROOT_TMP_PATH + "/many-folder-1";
        fs.mkdirSync(folderPath1);

        const folderPath2 = ROOT_TMP_PATH + "/many-folder-2";
        fs.mkdirSync(folderPath2);

        fs.writeFileSync(folderPath1 + "/func1.sql", `
            create or replace function func1()
            returns text as $body$
            begin
                return 'func1';
            end
            $body$
            language plpgsql;
        `);
        fs.writeFileSync(folderPath2 + "/func2.sql", `
            create or replace function func2()
            returns text as $body$
            begin
                return 'func2';
            end
            $body$
            language plpgsql;
        `);

        await DDLManager.build({
            db, 
            folder: [
                folderPath1, 
                folderPath2
            ]
        });


        const result = await db.query(`
            select 
                func1() as func1,
                func2() as func2
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            func1: "func1",
            func2: "func2"
        });
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
});