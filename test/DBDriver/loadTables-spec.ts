import {readDatabaseOptions} from "../utils";
import pg from "pg";
import {PgDBDriver} from "../../lib/db/PgDBDriver";
import fs from "fs";
import path from "path";
import assert from "assert";

describe("PgDBDriver: load tables", () => {
    const dbConfig = readDatabaseOptions();
    let db: pg.Client;
    let pgDriver: PgDBDriver;

    beforeEach(async() => {
        db = new pg.Client(dbConfig);
        await db.connect();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

        pgDriver = new PgDBDriver(dbConfig);
        await pgDriver.connect();
    });

    afterEach(async() => {
        db.end();
    });

    
    const fixturesPath = path.join(__dirname, "table-fixtures");
    const fixtures = fs.readdirSync(fixturesPath);

    for (const dirName of fixtures) {
        const dirPath = path.join(fixturesPath, dirName);

        const ddlPath = path.join(dirPath, "ddl.sql");
        const ddl = fs.readFileSync(ddlPath).toString();

        const resultPath = path.join(dirPath, "result");
        const expectedTablesJSON = require(resultPath);

        it(dirName, async() => {

            await db.query(ddl);
            
            const tables = await pgDriver.loadTables();
            const actualTablesJSON = tables.map(func => func.toJSON());

            assert.deepStrictEqual(
                actualTablesJSON,
                expectedTablesJSON
            );
        });
    }

});