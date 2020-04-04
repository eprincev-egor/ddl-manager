import pg from "pg";
import {readDatabaseOptions} from "../../utils";
import {PgDBDriver} from "../../../lib/db/PgDBDriver";
import { PgParser } from "../../../lib/parser/pg/PgParser";
import { FunctionModel } from "../../../lib/objects/FunctionModel";
import assert from "assert";

describe("PgDBDriver: drop/create function", () => {

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
    
    it("drop function", async() => {
        const sql = `
            create or replace function test_func()
            returns void as $body$
            begin
                
            end
            $body$
            language plpgsql;
        `;
        await db.query(sql);


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql)
        const functionModel = models[0] as FunctionModel;
        

        await pgDriver.dropFunction(functionModel);

        const result = await db.query(`
            select count(*)::integer as count
            from information_schema.routines as routines
            where
                routines.routine_name = 'test_func'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 0);

    });

    it("create function", async() => {
        const sql = `
            create or replace function test_func()
            returns void as $body$
            begin
                
            end
            $body$
            language plpgsql;
        `;


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql)
        const functionModel = models[0] as FunctionModel;
        
        await pgDriver.createFunction(functionModel);

        const result = await db.query(`
            select count(*)::integer as count
            from information_schema.routines as routines
            where
                routines.routine_name = 'test_func'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 1);

    });

});