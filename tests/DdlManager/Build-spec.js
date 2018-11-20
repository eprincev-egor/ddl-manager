"use strict";

const assert = require("assert");
const fs = require("fs");
const del = require("del");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../DdlManager");

const ROOT_TMP_PATH = __dirname + "/tmp";

before(() => {
    if ( !fs.existsSync(ROOT_TMP_PATH) ) {
        fs.mkdirSync(ROOT_TMP_PATH);
    }
});

describe("DdlManager.build", () => {
    
    it("build nonexistent folder", async() => {
        let db = await getDbClient();

        try {
            await DdlManager.build({
                db, 
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }

        db.end();
    });

    it("build empty folder", async() => {
        let db = await getDbClient();
        let folderPath = ROOT_TMP_PATH + "/empty";
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);

        await DdlManager.build({
            db, 
            folder: folderPath
        });

        // expected build without errors
        assert.ok(true);

        db.end();
        // clear state
        del.sync(folderPath);
    });

    it("build simple function", async() => {
        let db = await getDbClient();
        let folderPath = ROOT_TMP_PATH + "/simple-func";
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

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

        db.end();
        // clear state
        del.sync(folderPath);
    });

    it("build simple function", async() => {
        let db = await getDbClient();
        let folderPath = ROOT_TMP_PATH + "/simple-func";
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

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

        db.end();
        // clear state
        del.sync(folderPath);
    });

    it("replace function", async() => {
        let db = await getDbClient();
        let folderPath = ROOT_TMP_PATH + "/simple-func";
        let result;
        let row;
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

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

        db.end();
        // clear state
        del.sync(folderPath);
    });

    it("build simple trigger", async() => {
        let db = await getDbClient();
        let folderPath = ROOT_TMP_PATH + "/simple-trigger";
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);

        await db.query(`
            drop schema public cascade;
            create schema public;

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

        db.end();
        // clear state
        del.sync(folderPath);
    });

    it("replace trigger", async() => {
        let db = await getDbClient();
        let folderPath = ROOT_TMP_PATH + "/simple-trigger";
        let result;
        let row;
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);

        await db.query(`
            drop schema public cascade;
            create schema public;

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

        db.end();
        // clear state
        del.sync(folderPath);
    });

});