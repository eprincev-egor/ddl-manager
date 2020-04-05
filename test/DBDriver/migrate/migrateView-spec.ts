import pg from "pg";
import {readDatabaseOptions} from "../../utils";
import {PgDBDriver} from "../../../lib/db/PgDBDriver";
import { PgParser } from "../../../lib/parser/pg/PgParser";
import assert from "assert";
import { ViewModel } from "../../../lib/objects/ViewModel";

describe("PgDBDriver: drop/create view", () => {

    const dbConfig = readDatabaseOptions();
    let db: pg.Client;
    let pgDriver: PgDBDriver;

    before(async() => {
        db = new pg.Client(dbConfig);
        await db.connect();
    });
    
    beforeEach(async() => {
        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

        pgDriver = new PgDBDriver(dbConfig);
        await pgDriver.connect();
    })

    after(async() => {
        await db.end();
    });
    
    it("drop view", async() => {
        const sql = `
            create view test_view as select 1
        `;
        await db.query(sql);


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql)
        const viewModel = models[0] as ViewModel;
        

        await pgDriver.dropView(viewModel);

        const result = await db.query(`
            select count(*)::integer as count
            from information_schema.views as views
            where
            views.table_name = 'test_view'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 0);

    });

    it("create view", async() => {
        const sql = `
            create view test_view as select 1
        `;


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql)
        const viewModel = models[0] as ViewModel;
        
        await pgDriver.createView(viewModel);

        const result = await db.query(`
            select count(*)::integer as count
            from information_schema.views as views
            where
            views.table_name = 'test_view'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 1);

    });

});