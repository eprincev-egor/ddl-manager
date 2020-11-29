import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { flatMap } from "lodash";
import { getDBClient } from "../../getDbClient";
import { DDLManager } from "../../../../lib/DDLManager";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { PostgresDriver } from "../../../../lib/database/PostgresDriver";

use(chaiShallowDeepEqualPlugin);

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/DDLManager.build triggers", () => {
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

    it("recreate trigger function if has frozen triggers", async() => {
        await db.query(`
            create table companies (
                id serial primary key,
                name text
            );

            create or replace function my_super_frozen_func()
            returns trigger as $body$
            begin
                new.name = 'frozen';
                return new;
            end
            $body$
            language plpgsql;

            create trigger my_trigger
            before insert
            on companies
            for each row
            execute procedure my_super_frozen_func();
        `);

        const folderPath = ROOT_TMP_PATH + "/some-frozen-trigger";
        fs.mkdirSync(folderPath);

        fs.writeFileSync(folderPath + "/my_super_frozen_func.sql", `
            create or replace function my_super_frozen_func()
            returns trigger as $body$
            begin
                new.name = 'not frozen';
                return new;
            end
            $body$
            language plpgsql;
        `);

        await DDLManager.build({
            db,
            folder: folderPath
        });

        const result = await db.query(`
            insert into companies default values
            returning *
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            id: 1,
            name: "not frozen"
        });
    });

    it("change trigger and frozen status on build", async() => {
        // create frozen function
        await db.query(`
            create table companies (
                id serial primary key,
                name text
            );

            create or replace function my_super_frozen_func()
            returns trigger as $body$
            begin
                new.name = 'frozen';
                return new;
            end
            $body$
            language plpgsql;

            create trigger my_trigger
            before insert
            on companies
            for each row
            execute procedure my_super_frozen_func();
        `);

        fs.writeFileSync(ROOT_TMP_PATH + "/func1.sql", `
            create or replace function my_super_frozen_func()
            returns trigger as $body$
            begin
                new.name = 'frozen';
                return new;
            end
            $body$
            language plpgsql;


            create trigger my_trigger
            before insert
            on companies
            for each row
            execute procedure my_super_frozen_func();
        `);

        await DDLManager.build({
            db, 
            folder: ROOT_TMP_PATH,
            throwError: false
        });

        const postgres = new PostgresDriver(db);
        const triggers = flatMap(
            (await postgres.load()).tables,
            table => table.triggers
        );

        expect(triggers[0]).to.be.shallowDeepEqual({
            name: "my_trigger",
            frozen: false
        });
    });

});