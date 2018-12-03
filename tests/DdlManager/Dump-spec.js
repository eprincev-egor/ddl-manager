"use strict";

const assert = require("assert");
const fs = require("fs");
const del = require("del");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../lib/DdlManager");
const DDLCoach = require("../../lib/parser/DDLCoach");

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("DdlManager.dump", () => {
    let db;
    
    beforeEach(async() => {
        db = await getDbClient();

        await db.query(`
            drop schema public cascade;
            create schema public;

            drop schema if exists test cascade;
        `);

        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            del.sync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });

    afterEach(async() => {
        db.end();
    });

    it("dump nonexistent folder", async() => {
        try {
            await DdlManager.dump({
                db, 
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("dump empty db", async() => {

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        // expected dump without errors
        assert.ok(true);
    });

    it("dump simple function", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;
        `);

        await DdlManager.dump({
            db: {
                database: db.database,
                user: db.user,
                password: db.password,
                host: db.host,
                port: db.port
            }, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
            function: {
                schema: "public",
                name: "simple_func",
                returns: {type: "integer"},
                language: "sql",
                args: [],
                body: "select 1"
            }
        });
    });

    it("dump simple function from some schema", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;

            create schema test;

            create or replace function test.simple_func()
            returns integer as $$select 1$$
            language sql;
        `);

        await DdlManager.dump({
            db: {
                database: db.database,
                user: db.user,
                password: db.password,
                host: db.host,
                port: db.port
            }, 
            folder: ROOT_TMP_PATH
        });

        let sql;
        let content;

        sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
            function: {
                schema: "public",
                name: "simple_func",
                returns: {type: "integer"},
                language: "sql",
                args: [],
                body: "select 1"
            }
        });

        sql = fs.readFileSync(ROOT_TMP_PATH + "/test/simple_func.sql").toString();
        content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
            function: {
                schema: "test",
                name: "simple_func",
                returns: {type: "integer"},
                language: "sql",
                args: [],
                body: "select 1"
            }
        });
    });

    it("dump simple function and trigger", async() => {
        let body = `
            begin
                return new;
            end
        `;
        await db.query(`
            create table company (
                id serial primary key
            );

            create or replace function some_func()
            returns trigger as $body$${ body }$body$
            language plpgsql;

            create trigger some_trigger
            after insert or update or delete
            on company
            for each row
            execute procedure some_func();
        `);

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/company/some_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
            function: {
                schema: "public",
                name: "some_func",
                returns: {type: "trigger"},
                language: "plpgsql",
                args: [],
                body
            },
            trigger: {
                table: {
                    schema: "public",
                    name: "company"
                },
                after: true,
                insert: true,
                update: true,
                delete: true,
                name: "some_trigger",
                procedure: {
                    schema: "public",
                    name: "some_func"
                }
            }
        });
    });

    it("dump function from public and trigger table from test schema, file must be in folder test/company", async() => {
        let body = `
            begin
                return new;
            end
        `;
        await db.query(`
            create schema test;

            create table test.company (
                id serial primary key
            );

            create or replace function public.some_func()
            returns trigger as $body$${ body }$body$
            language plpgsql;

            create trigger some_trigger
            after insert or update or delete
            on test.company
            for each row
            execute procedure some_func();
        `);

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/test/company/some_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
            function: {
                schema: "public",
                name: "some_func",
                returns: {type: "trigger"},
                language: "plpgsql",
                args: [],
                body
            },
            trigger: {
                table: {
                    schema: "test",
                    name: "company"
                },
                after: true,
                insert: true,
                update: true,
                delete: true,
                name: "some_trigger",
                procedure: {
                    schema: "public",
                    name: "some_func"
                }
            }
        });
    });

});