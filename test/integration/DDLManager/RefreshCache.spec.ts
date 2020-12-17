import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../getDbClient";
import { DDLManager } from "../../../lib/DDLManager";
import { sleep } from "../sleep";

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


    it("refresh cache values if no changes in triggers", async() => {
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
            create table cargos (
                id serial primary key,
                id_order integer,
                gross_weight numeric
            );
            insert into companies default values;
            insert into companies default values;

            insert into orders (id_client) values (1);
            insert into orders (id_client) values (1);

            insert into cargos (id_order, gross_weight) values (1, 200);
            insert into cargos (id_order, gross_weight) values (2, 400);
        `);

        fs.writeFileSync(folderPath + "/companies_cache.sql", `
            cache totals for companies (
                select
                    count(*) as orders_count,
                    sum( orders.cargos_gross_weight ) as cargos_gross_weight
                
                from orders
                where
                    orders.id_client = companies.id
            )
        `);
        fs.writeFileSync(folderPath + "/orders_cache.sql", `
            cache totals for orders (
                select
                    sum( cargos.gross_weight ) as cargos_gross_weight
                
                from cargos
                where
                    cargos.id_order = orders.id
            )
        `);
        await DDLManager.build({
            db, 
            folder: folderPath,
            throwError: true
        });

        // broke data
        await db.query(`
            alter table orders
                disable trigger all;
            alter table companies
                disable trigger all;

            update orders set
                cargos_gross_weight = -1;
            update companies set
                orders_count = -1,
                cargos_gross_weight = -1;
        `);
        await db.query(`
            alter table orders
                enable trigger all;
            alter table companies
                enable trigger all;
        `);


        await DDLManager.refreshCache({
            db, 
            folder: folderPath
        });


        let result;

        result = await db.query(`
            select id, orders_count, cargos_gross_weight
            from companies 
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, orders_count: "2", cargos_gross_weight: "600"},
            {id: 2, orders_count: "0", cargos_gross_weight: "0"}
        ]);
        result = await db.query(`
            select id, cargos_gross_weight 
            from orders
            order by id
        `);
        assert.deepStrictEqual(result.rows, [
            {id: 1, cargos_gross_weight: "200"},
            {id: 2, cargos_gross_weight: "400"}
        ]);
    });

});