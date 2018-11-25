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
        
    before(() => {
        if ( !fs.existsSync(ROOT_TMP_PATH) ) {
            fs.mkdirSync(ROOT_TMP_PATH);
        }
    });

    it("watch nonexistent folder", async() => {
        let db = await getDbClient();

        try {
            await DdlManager.watch({
                db, 
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }

        db.end();
    });

    it("watch empty folder", async() => {
        let db = await getDbClient();
        let folderPath = ROOT_TMP_PATH + "/empty";
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);

        await DdlManager.watch({
            db, 
            folder: folderPath
        });

        // expected build without errors
        assert.ok(true);

        DdlManager.stopWatch();

        db.end();
        // clear state
        del.sync(folderPath);
    });


    it("watch simple function", async() => {
        let db = await getDbClient();
        let folderPath = ROOT_TMP_PATH + "/watch-func";
        let result;
        let row;
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);
        fs.chmodSync(folderPath, parseInt("777", 8));

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

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

        db.end();

        // stop watching before delete folder
        DdlManager.stopWatch();

        // clear state
        del.sync(folderPath);
    });

});