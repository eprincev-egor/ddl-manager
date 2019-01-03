"use strict";

const assert = require("assert");
const fs = require("fs");
const del = require("del");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../lib/DdlManager");

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("DdlManager.build", () => {
    let db;
    
    beforeEach(async() => {
        db = await getDbClient();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            del.sync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });

    afterEach(async() => {
        db.end();
    });

    it("build nonexistent folder", async() => {
        try {
            await DdlManager.build({
                db, 
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("build empty folder", async() => {
        let folderPath = ROOT_TMP_PATH + "/empty";
        fs.mkdirSync(folderPath);

        await DdlManager.build({
            db, 
            folder: folderPath
        });

        // expected build without errors
        assert.ok(true);
    });

    it("build simple function", async() => {
        let folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        let rnd = Math.round( 1000 * Math.random() );
        fs.writeFileSync(folderPath + "/nice.sql", `
            create or replace function nice(a integer)
            returns integer as $body$
                begin
                    return a * ${ rnd };
                end
            $body$
            language plpgsql;
        `);


        await DdlManager.build({
            db, 
            folder: folderPath
        });

        let result = await db.query("select nice(2) as nice");
        let row = result.rows[0];

        assert.deepEqual(row, {
            nice: 2 * rnd
        });
    });

    it("build with dbConfig", async() => {
        let folderPath = ROOT_TMP_PATH + "/simple-func";
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


        await DdlManager.build({
            db: {
                database: db.database,
                user: db.user,
                password: db.password,
                host: db.host,
                port: db.port
            }, 
            folder: folderPath
        });

        let result = await db.query("select nice() as nice");
        let row = result.rows[0];

        assert.deepEqual(row, {
            nice: 1
        });
    });

    it("replace function", async() => {
        let folderPath = ROOT_TMP_PATH + "/simple-func";
        let result;
        let row;
    
        fs.mkdirSync(folderPath);

        let rnd = Math.round( 1000 * Math.random() );
        fs.writeFileSync(folderPath + "/nice.sql", `
            create or replace function nice(a integer)
            returns integer as $body$
                begin
                    return a * ${ rnd };
                end
            $body$
            language plpgsql;
        `);


        await DdlManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("select nice(2) as nice");
        row = result.rows[0];

        assert.deepEqual(row, {
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

        await DdlManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("select nice() as nice");
        row = result.rows[0];

        assert.deepEqual(row, {
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
        let folderPath = ROOT_TMP_PATH + "/simple-trigger";
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


        await DdlManager.build({
            db, 
            folder: folderPath
        });

        let result = await db.query("insert into company (name) values ('super') returning note");
        let row = result.rows[0];

        assert.deepEqual(row, {
            note: "name: super"
        });
    });

    it("replace trigger", async() => {
        let folderPath = ROOT_TMP_PATH + "/simple-trigger";
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


        await DdlManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("insert into company (name) values ('super') returning note");
        row = result.rows[0];

        assert.deepEqual(row, {
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


        await DdlManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("insert into company (name) values ('test') returning note");
        row = result.rows[0];

        assert.deepEqual(row, {
            note: "nice: test"
        });
    });

    
    it("remove function", async() => {
        let folderPath = ROOT_TMP_PATH + "/simple-func";
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


        await DdlManager.build({
            db, 
            folder: folderPath
        });

        let result = await db.query("select nice() as nice");
        let row = result.rows[0];

        assert.deepEqual(row, {
            nice: 1
        });


        // remove file for remove function from db
        fs.unlinkSync(folderPath + "/nice.sql");

        await DdlManager.build({
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
        let folderPath = ROOT_TMP_PATH + "/simple-func";
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


        await DdlManager.build({
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

        await DdlManager.build({
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


        await DdlManager.build({
            db, 
            folder: ROOT_TMP_PATH
        });

        let result = await db.query("select func2(1) as func2");
        let row = result.rows[0];

        assert.deepEqual(row, {
            func2: 2
        });

        try {
            await db.query("select func1(1)");
            throw new Error("expected error");
        } catch(err) {
            assert.equal(err.message, "function func1(integer) does not exist");
        }
    });


    it("build two functions, one freeze error, one success", async() => {
        // create freeze function
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


        await DdlManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: false
        });

        let result;

        result = await db.query("select func2(1) as func2");

        assert.deepEqual(result.rows[0], {
            func2: 2
        });

        result = await db.query("select func1(1) as func1");

        assert.deepEqual(result.rows[0], {
            func1: 0
        });
    });

    it("build simple function with comment", async() => {
        let folderPath = ROOT_TMP_PATH + "/simple-func";
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


        await DdlManager.build({
            db, 
            folder: folderPath
        });

        let result = await db.query(`
            select
                pg_catalog.obj_description( pg_proc.oid ) as comment
            from information_schema.routines as routines

            left join pg_catalog.pg_proc as pg_proc on
                routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text
        
            where
                routines.routine_schema = 'public' and
                routines.routine_name = 'nice'
        `);

        assert.deepEqual(result.rows[0], {
            comment: "good\nddl-manager-sync"
        });
    });

    it("drop function with comment, after dump", async() => {
        let folderPath = ROOT_TMP_PATH + "/simple-trigger";
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

        assert.deepEqual(row, {
            test: 1
        });

        await DdlManager.dump({
            db,
            folder: folderPath,
            unfreeze: true
        });

        // remove file with trigger
        fs.unlinkSync(folderPath + "/public/test.sql");
        
        // drop trigger by build command
        await DdlManager.build({
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
        let folderPath = ROOT_TMP_PATH + "/simple-trigger";
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

        assert.deepEqual(row, {
            note: "name: super"
        });

        await DdlManager.dump({
            db,
            folder: folderPath,
            unfreeze: true
        });

        // remove file with trigger
        fs.unlinkSync(folderPath + "/public/company/set_note_before_insert_or_update_name.sql");
        
        // drop trigger by build command
        await DdlManager.build({
            db, 
            folder: folderPath
        });

        result = await db.query("insert into company (name) values ('super 2') returning note");
        row = result.rows[0];

        assert.deepEqual(row, {
            note: null
        });
    });

    it("build function with comment, after dump, comment must be exists", async() => {
        let folderPath = ROOT_TMP_PATH + "/simple-trigger";
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

        assert.deepEqual(row, {
            test: 1
        });

        await DdlManager.dump({
            db,
            folder: folderPath,
            unfreeze: true
        });

        await DdlManager.build({
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

        assert.deepEqual(result.rows[0], {
            comment: "test\nddl-manager-sync"
        });
    });

    it("build trigger with comment, after dump, comment must be exists", async() => {
        let folderPath = ROOT_TMP_PATH + "/simple-trigger";
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

        assert.deepEqual(row, {
            note: "name: super"
        });

        await DdlManager.dump({
            db,
            folder: folderPath,
            unfreeze: true
        });

        await DdlManager.build({
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

        assert.deepEqual(result.rows[0], {
            comment: "test\nddl-manager-sync"
        });
    });
});