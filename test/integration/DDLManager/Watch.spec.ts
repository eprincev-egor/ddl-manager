import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../getDbClient";
import { DDLManager } from "../../../lib/DDLManager";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { sleep } from "../sleep";

use(chaiShallowDeepEqualPlugin);

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/DDLManager.watch", () => {
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
        DDLManager.stopWatch();
    });

    it("watch nonexistent folder", async() => {
        try {
            await DDLManager.watch({
                db, 
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("watch empty folder", async() => {
        const folderPath = ROOT_TMP_PATH + "/empty";
        fs.mkdirSync(folderPath);

        await DDLManager.watch({
            db, 
            folder: folderPath
        });

        // expected build without errors
        assert.ok(true);
    });


    it("watch simple function", async() => {
        const folderPath = ROOT_TMP_PATH + "/watch-func";
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


        await DDLManager.watch({
            db, 
            folder: folderPath
        });

        result = await db.query("select some_func() as some_func");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
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
        
        await sleep(150);

        result = await db.query("select some_func() as some_func");
        row = result.rows[0];


        expect(row).to.be.shallowDeepEqual({
            some_func: 2
        });
    });

    it("watch with dbConfig", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        await DDLManager.watch({
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

        const result = await db.query("select nice() as nice");
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            nice: 1
        });
    });

    it("watch file with error, and without", async() => {

        await DDLManager.watch({
            db, 
            folder: ROOT_TMP_PATH
        });

        await DDLManager.watch({
            db, 
            folder: ROOT_TMP_PATH
        });

        // file with syntax error
        fs.writeFileSync(ROOT_TMP_PATH + "/test_errors.sql", `
            create or replace
        `);
        await sleep(150);

        // file without syntax error
        fs.writeFileSync(ROOT_TMP_PATH + "/test_errors.sql", `
            create or replace function test()
            returns integer as $$select 1$$
            language sql;
        `);
        await sleep(150);

        const result = await db.query("select test() as nice");
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            nice: 1
        });
    });


    
    it("drop frozen function, if she was replaced by ddl-manager", async() => {
        let result;
        let row;

        await DDLManager.watch({
            db, 
            folder: ROOT_TMP_PATH
        });

        // create frozen function
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
        await sleep(150);

        result = await db.query("select test() as test");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            test: 2
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
        await sleep(150);

        // new function must be created
        result = await db.query("select test2() as test2");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            test2: 2
        });

        // frozen function should be dropped
        try {
            await db.query("select test() as test");
            assert.ok(false, "function test() was not dropped");
        } catch(err) {
            assert.ok(true);
        }
    });

    it("watch folder '/../some' ", async() => {
        const folderPath = ROOT_TMP_PATH + "/watch";
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


        await DDLManager.watch({
            db, 
            folder: folderPath + "/../watch"
        });

        result = await db.query("select some_func() as some_func");
        row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
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


        expect(row).to.be.shallowDeepEqual({
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


        expect(row).to.be.shallowDeepEqual({
            some_func: 3
        });
    });

    it("watch from many folders", async() => {

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

        await DDLManager.watch({
            db, 
            folder: [
                folderPath1, 
                folderPath2
            ]
        });


        let result = await db.query(`
            select 
                func1() as func1,
                func2() as func2
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            func1: "func1",
            func2: "func2"
        });


        fs.writeFileSync(folderPath1 + "/func1.sql", `
            create or replace function func1()
            returns text as $body$
            begin
                return 'changed func1';
            end
            $body$
            language plpgsql;
        `);
        fs.writeFileSync(folderPath2 + "/func2.sql", `
            create or replace function func2()
            returns text as $body$
            begin
                return 'changed func2';
            end
            $body$
            language plpgsql;
        `);
        await sleep(100);

        result = await db.query(`
            select 
                func1() as func1,
                func2() as func2
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            func1: "changed func1",
            func2: "changed func2"
        });
    });

    it("don't update cache columns in watcher mode (cpu high load)", async() => {
        const folderPath = ROOT_TMP_PATH + "/watch-cache";
    
        
        await db.query(`
            create table companies (
                id serial primary key
            );
            create table orders (
                id serial primary key,
                id_client integer
            );
            insert into companies default values;
            insert into companies default values;

            insert into orders (id_client) values (1);
            insert into orders (id_client) values (1);
        `);

        fs.mkdirSync(folderPath);

        await DDLManager.watch({
            db, 
            folder: folderPath
        });

        fs.writeFileSync(folderPath + "/watch_cache.sql", `
            cache totals for companies (
                select count(*) as orders_count
                from orders
                where orders.id_client = companies.id
            )
        `);
        await sleep(100);


        const result = await db.query(`
            select id, orders_count 
            from companies 
            order by id
        `);

        assert.deepStrictEqual(result.rows, [
            {id: 1, orders_count: "0"},
            {id: 2, orders_count: "0"}
        ]);
    });

});