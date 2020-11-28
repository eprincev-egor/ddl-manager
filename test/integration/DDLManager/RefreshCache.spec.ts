import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../getDbClient";
import { DDLManager } from "../../../lib/DDLManager";
import { FileParser } from "../../../lib/parser/FileParser";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { sleep } from "../sleep";


use(chaiShallowDeepEqualPlugin);

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/DDLManager.refreshCache", () => {
    let db: any;
    
    beforeEach(async() => {
        db = await getDBClient();

        await db.query(`
            drop schema public cascade;
            create schema public;

            drop schema if exists test cascade;
        `);

        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });

    afterEach(async() => {
        db.end();
    });

    it("refresh simple cache", async() => {
        const folderPath = ROOT_TMP_PATH + "/some cache";
        fs.mkdirSync(folderPath);

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

        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });
        fs.writeFileSync(folderPath + "/some_cache.sql", `
            cache totals for companies (
                select
                    count(*) as orders_count
                
                from orders
                where
                    orders.id_client = companies.id
            )
        `);
        await sleep(100);

        await DDLManager.refreshCache({
            db, 
            folder: folderPath
        });

        const result = await db.query(`
            select id, orders_count 
            from companies 
            order by id
        `);

        assert.deepStrictEqual(result.rows, [
            {id: 1, orders_count: "2"},
            {id: 2, orders_count: "0"}
        ]);
    });


});