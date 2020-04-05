import pg from "pg";
import {readDatabaseOptions} from "../../utils";
import {PgDBDriver} from "../../../lib/db/PgDBDriver";
import { PgParser } from "../../../lib/parser/pg/PgParser";
import assert from "assert";
import { TableModel } from "../../../lib/objects/TableModel";

describe("PgDBDriver: drop/create column NotNull", () => {

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
    
    it("drop column NotNull", async() => {
        const sql = `
            create table test_table (
                id integer not null
            );
        `;
        await db.query(sql);


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql)
        const tableModel = models[0] as TableModel;
        const columnModel = tableModel.row.columns[0];
        

        await pgDriver.dropColumnNotNull(tableModel, columnModel);

        const result = await db.query(`
            select
                pg_columns.is_nullable
            from information_schema.columns as pg_columns

            where
                pg_columns.table_schema = 'public' and
                pg_columns.table_name = 'test_table' and
                pg_columns.column_name = 'id'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.is_nullable, "YES");

    });

    it("create column default", async() => {
        const initialStateSQL = `
            create table test_table (
                id integer
            );
        `;
        await db.query(initialStateSQL);

        const sql = `
            create table test_table (
                id integer not null
            )
        `;


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql);
        const tableModel = models[0] as TableModel;
        const columnModel = tableModel.row.columns[0];
        
        await pgDriver.createColumnNotNull(tableModel, columnModel);

        const result = await db.query(`
            select
                pg_columns.is_nullable
            from information_schema.columns as pg_columns

            where
                pg_columns.table_schema = 'public' and
                pg_columns.table_name = 'test_table' and
                pg_columns.column_name = 'id'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.is_nullable, "NO");

    });

});