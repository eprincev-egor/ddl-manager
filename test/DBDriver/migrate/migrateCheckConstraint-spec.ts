import pg from "pg";
import {readDatabaseOptions} from "../../utils";
import {PgDBDriver} from "../../../lib/db/PgDBDriver";
import { PgParser } from "../../../lib/parser/pg/PgParser";
import assert from "assert";
import { TableModel } from "../../../lib/objects/TableModel";

describe("PgDBDriver: drop/create check constraint", () => {

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
    
    it("drop check constraint", async() => {
        const sql = `
            create table test_table (
                id integer check (id > 0)
            );
        `;
        await db.query(sql);


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql)
        const tableModel = models[0] as TableModel;
        const checkConstraintModel = tableModel.row.checkConstraints[0];
        

        await pgDriver.dropCheckConstraint(tableModel, checkConstraintModel);

        const result = await db.query(`
            select
                count(*)::integer as count

            from information_schema.table_constraints as tc

            left join information_schema.check_constraints as check_info on
                check_info.constraint_schema = tc.table_schema and
                check_info.constraint_name = tc.constraint_name
            
            where
                tc.table_schema = 'public' and
                tc.table_name = 'test_table' and
                tc.constraint_type = 'CHECK' and
                tc.constraint_name not ilike '%_not_null' and
                check_info.check_clause not ilike '% IS NOT NULL'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 0);

    });

    it("create check constraint (from column)", async() => {
        const initialStateSQL = `
            create table test_table (
                id integer
            );
        `;
        await db.query(initialStateSQL);

        const sql = `
            create table test_table (
                id integer check (id > 0)
            )
        `;


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql);
        const tableModel = models[0] as TableModel;
        const checkConstraintModel = tableModel.row.checkConstraints[0];
        
        await pgDriver.createCheckConstraint(tableModel, checkConstraintModel);

        const result = await db.query(`
            select
                count(*)::integer as count

            from information_schema.table_constraints as tc

            left join information_schema.check_constraints as check_info on
                check_info.constraint_schema = tc.table_schema and
                check_info.constraint_name = tc.constraint_name
            
            where
                tc.table_schema = 'public' and
                tc.table_name = 'test_table' and
                tc.constraint_type = 'CHECK' and
                tc.constraint_name not ilike '%_not_null' and
                check_info.check_clause not ilike '% IS NOT NULL'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 1);

    });

    it("create check constraint (from constraints clause)", async() => {
        const initialStateSQL = `
            create table test_table (
                id integer
            );
        `;
        await db.query(initialStateSQL);

        const sql = `
            create table test_table (
                id integer, 
                constraint test_table_check 
                    check (id > 0)
            )
        `;


        const parser = new PgParser();
        const models = parser.parseFile("test.sql", sql);
        const tableModel = models[0] as TableModel;
        const checkConstraintModel = tableModel.row.checkConstraints[0];
        
        await pgDriver.createCheckConstraint(tableModel, checkConstraintModel);

        const result = await db.query(`
            select
                count(*)::integer as count

            from information_schema.table_constraints as tc

            left join information_schema.check_constraints as check_info on
                check_info.constraint_schema = tc.table_schema and
                check_info.constraint_name = tc.constraint_name
            
            where
                tc.table_schema = 'public' and
                tc.table_name = 'test_table' and
                tc.constraint_type = 'CHECK' and
                tc.constraint_name not ilike '%_not_null' and
                check_info.check_clause not ilike '% IS NOT NULL'
        `);
        const row = result.rows[0];
        assert.strictEqual(row.count, 1);

    });
});