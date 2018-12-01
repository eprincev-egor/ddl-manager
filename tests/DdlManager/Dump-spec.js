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

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/simple_func.sql").toString();
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

});