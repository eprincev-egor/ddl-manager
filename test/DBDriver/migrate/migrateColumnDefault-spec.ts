import pg from "pg";
import {readDatabaseOptions} from "../../utils";
import {PgDBDriver} from "../../../lib/db/PgDBDriver";
import { PgParser } from "../../../lib/parser/pg/PgParser";
import assert from "assert";
import { TableModel } from "../../../lib/objects/TableModel";

describe("PgDBDriver: drop/create column default", () => {

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
    
    it("drop column default", async() => {
        const sql = `
            create table test_table (
                id integer default 1
            );
        `;
        await db.query(sql);


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql)
        const tableModel = models[0] as TableModel;
        const columnModel = tableModel.row.columns[0];
        

        await pgDriver.dropColumnDefault(tableModel, columnModel);

        const result = await db.query(`
            insert into test_table (
                id
            ) values (
                default
            )
            returning id
        `);
        const row = result.rows[0];
        assert.strictEqual(row.id, null);

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
                id integer default 100
            )
        `;


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql);
        const tableModel = models[0] as TableModel;
        const columnModel = tableModel.row.columns[0];
        
        await pgDriver.createColumnDefault(tableModel, columnModel);

        const result = await db.query(`
            insert into test_table (
                id
            ) values (
                default
            )
            returning id
        `);
        const row = result.rows[0];
        assert.strictEqual(row.id, 100);

    });

});