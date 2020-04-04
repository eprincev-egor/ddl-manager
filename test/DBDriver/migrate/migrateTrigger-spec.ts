import pg from "pg";
import {readDatabaseOptions} from "../../utils";
import {PgDBDriver} from "../../../lib/db/PgDBDriver";
import { PgParser } from "../../../lib/parser/pg/PgParser";
import { TriggerModel } from "../../../lib/objects/TriggerModel";
import assert from "assert";

describe("PgDBDriver: drop/create trigger", () => {

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
    
    it("drop trigger", async() => {
        const sql = `
            create table test_table (
                id integer
            );

            create or replace function test_func()
            returns trigger as $body$
            begin
                
            end
            $body$
            language plpgsql;

            create trigger test_trigger
            after insert
            on test_table
            for each row
            execute procedure test_func()
        `;
        await db.query(sql);


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql)
        const triggerModel = models[2] as TriggerModel;
        

        await pgDriver.dropTrigger(triggerModel);

        const result = await db.query(`
            select count(*)::integer as count
            from pg_trigger
            where
                pg_trigger.tgname = 'test_trigger'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 0);

    });

    it("create function", async() => {
        const initialStateSQL = `
            create table test_table (
                id integer
            );

            create or replace function test_func()
            returns trigger as $body$
            begin
                
            end
            $body$
            language plpgsql;
        `;
        await db.query(initialStateSQL);

        const sql = `
            create trigger test_trigger
            after insert
            on test_table
            for each row
            execute procedure test_func()
        `;


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql)
        const triggerModel = models[0] as TriggerModel;
        
        await pgDriver.createTrigger(triggerModel);

        const result = await db.query(`
            select count(*)::integer as count
            from pg_trigger
            where
                pg_trigger.tgname = 'test_trigger'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 1);

    });

});