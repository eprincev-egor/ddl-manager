import pg from "pg";
import {readDatabaseOptions} from "../../utils";
import {PgDBDriver} from "../../../lib/db/PgDBDriver";
import { PgParser } from "../../../lib/parser/pg/PgParser";
import assert from "assert";
import { TableModel } from "../../../lib/objects/TableModel";

describe("PgDBDriver: drop/create primary key", () => {

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
    
    it("drop primary key", async() => {
        const sql = `
            create table test_table (
                id integer primary key
            );
        `;
        await db.query(sql);


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql)
        const tableModel = models[0] as TableModel;
        
        await pgDriver.dropPrimaryKey(tableModel);

        const result = await db.query(`
            select
                count(*)::integer as count

            from information_schema.table_constraints as tc

            where
                tc.table_schema = 'public' and
                tc.table_name = 'test_table' and
                tc.constraint_type = 'PRIMARY KEY'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 0);

    });

    it("create primary key (from column)", async() => {
        const initialStateSQL = `
            create table test_table (
                id integer
            );
        `;
        await db.query(initialStateSQL);

        const sql = `
            create table test_table (
                id integer primary key
            )
        `;


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql);
        const tableModel = models[0] as TableModel;
        const primaryKey = tableModel.row.primaryKey;
        
        await pgDriver.createPrimaryKey(tableModel, primaryKey);

        const result = await db.query(`
            select
                count(*)::integer as count

            from information_schema.table_constraints as tc

            where
                tc.table_schema = 'public' and
                tc.table_name = 'test_table' and
                tc.constraint_type = 'PRIMARY KEY'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 1);

    });

    it("create primary key (from constraints clause)", async() => {
        const initialStateSQL = `
            create table test_table (
                id integer
            );
        `;
        await db.query(initialStateSQL);

        const sql = `
            create table test_table (
                id integer, 
                constraint test_table_pk
                    primary key (id)
            )
        `;


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql);
        const tableModel = models[0] as TableModel;
        const primaryKey = tableModel.row.primaryKey;
        
        await pgDriver.createPrimaryKey(tableModel, primaryKey);

        const result = await db.query(`
            select
                count(*)::integer as count

            from information_schema.table_constraints as tc

            where
                tc.table_schema = 'public' and
                tc.table_name = 'test_table' and
                tc.constraint_type = 'PRIMARY KEY'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 1);

    });
});