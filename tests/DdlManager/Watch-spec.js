"use strict";

const assert = require("assert");
const fs = require("fs");
const del = require("del");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../lib/DdlManager");

const ROOT_TMP_PATH = __dirname + "/tmp";

async function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

describe("DdlManager.watch", () => {
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
        DdlManager.stopWatch();
    });

    it("watch nonexistent folder", async() => {
        try {
            await DdlManager.watch({
                db, 
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("watch empty folder", async() => {
        let folderPath = ROOT_TMP_PATH + "/empty";
        fs.mkdirSync(folderPath);

        await DdlManager.watch({
            db, 
            folder: folderPath
        });

        // expected build without errors
        assert.ok(true);
    });


    it("watch simple function", async() => {
        let folderPath = ROOT_TMP_PATH + "/watch-func";
        let result;
        let row;
    
        fs.mkdirSync(folderPath);

        fs.writeFileSync(folderPath + "/watch_test.sql", `
            create or replace function some_func()
            returns integer as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;
        `);


        await DdlManager.watch({
            db, 
            folder: folderPath
        });

        result = await db.query("select some_func() as some_func");
        row = result.rows[0];

        assert.deepEqual(row, {
            some_func: 1
        });

        // change function
        fs.writeFileSync(folderPath + "/watch_test.sql", `
            create or replace function some_func()
            returns integer as $body$
                begin
                    return 2;
                end
            $body$
            language plpgsql;
        `);
        
        await sleep(100);

        result = await db.query("select some_func() as some_func");
        row = result.rows[0];


        assert.deepEqual(row, {
            some_func: 2
        });
    });

    it("watch with dbConfig", async() => {
        let folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        await DdlManager.watch({
            db: {
                database: db.database,
                user: db.user,
                password: db.password,
                host: db.host,
                port: db.port
            }, 
            folder: folderPath
        });

        fs.writeFileSync(folderPath + "/nice.sql", `
            create or replace function nice()
            returns integer as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;
        `);

        await sleep(150);

        let result = await db.query("select nice() as nice");
        let row = result.rows[0];

        assert.deepEqual(row, {
            nice: 1
        });
    });

    it("watch file with error, and without", async() => {

        await DdlManager.watch({
            db, 
            folder: ROOT_TMP_PATH
        });

        await DdlManager.watch({
            db, 
            folder: ROOT_TMP_PATH
        });

        // file with syntax error
        fs.writeFileSync(ROOT_TMP_PATH + "/test_errors.sql", `
            create or replace
        `);
        await sleep(50);

        // file without syntax error
        fs.writeFileSync(ROOT_TMP_PATH + "/test_errors.sql", `
            create or replace function test()
            returns integer as $$select 1$$
            language sql;
        `);
        await sleep(50);

        let result = await db.query("select test() as nice");
        let row = result.rows[0];

        assert.deepEqual(row, {
            nice: 1
        });
    });


    
    it("don't drop freeze function", async() => {
        let result;
        let row;

        await DdlManager.watch({
            db, 
            folder: ROOT_TMP_PATH
        });

        // create freeze function
        await db.query(`
            create or replace function test()
            returns integer as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;
        `);

        // create function with same function identify
        fs.writeFileSync(ROOT_TMP_PATH + "/test.sql", `
            create or replace function test()
            returns integer as $body$
                begin
                    return 2;
                end
            $body$
            language plpgsql;
        `);
        await sleep(50);

        result = await db.query("select test() as test");
        row = result.rows[0];

        assert.deepEqual(row, {
            test: 1
        });

        // change function name
        fs.writeFileSync(ROOT_TMP_PATH + "/test.sql", `
            create or replace function test2()
            returns integer as $body$
                begin
                    return 2;
                end
            $body$
            language plpgsql;
        `);
        await sleep(50);

        // new function must be created
        result = await db.query("select test2() as test2");
        row = result.rows[0];

        assert.deepEqual(row, {
            test2: 2
        });

        // freeze function should not be dropped
        result = await db.query("select test() as test");
        row = result.rows[0];

        assert.deepEqual(row, {
            test: 1
        });
    });

    it("watch folder '/../some' ", async() => {
        let folderPath = ROOT_TMP_PATH + "/watch";
        let result;
        let row;
    
        fs.mkdirSync(folderPath);

        fs.writeFileSync(folderPath + "/watch_test.sql", `
            create or replace function some_func()
            returns integer as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;
        `);


        await DdlManager.watch({
            db, 
            folder: folderPath + "/../watch"
        });

        result = await db.query("select some_func() as some_func");
        row = result.rows[0];

        assert.deepEqual(row, {
            some_func: 1
        });

        // change function
        fs.writeFileSync(folderPath + "/watch_test.sql", `
            create or replace function some_func()
            returns integer as $body$
                begin
                    return 2;
                end
            $body$
            language plpgsql;
        `);
        
        await sleep(100);

        result = await db.query("select some_func() as some_func");
        row = result.rows[0];


        assert.deepEqual(row, {
            some_func: 2
        });

        
        // change function again, but by another path
        fs.writeFileSync(folderPath + "/../watch/watch_test.sql", `
            create or replace function some_func()
            returns integer as $body$
                begin
                    return 3;
                end
            $body$
            language plpgsql;
        `);
        
        await sleep(100);

        result = await db.query("select some_func() as some_func");
        row = result.rows[0];


        assert.deepEqual(row, {
            some_func: 3
        });
    });

});