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

});