import {readDatabaseOptions} from "../utils";
import pg from "pg";
import {PgDBDriver} from "../../lib/db/PgDBDriver";
import fs from "fs";
import path from "path";
import assert from "assert";

describe("PgDBDriver: load functions", () => {
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

    
    const fixturesPath = path.join(__dirname, "func-fixtures");
    const fixtures = fs.readdirSync(fixturesPath);

    for (const dirName of fixtures) {
        const dirPath = path.join(fixturesPath, dirName);

        const ddlPath = path.join(dirPath, "ddl.sql");
        const ddlBuffer = fs.readFileSync(ddlPath);
        const ddl = fixLineBreaks( ddlBuffer.toString() );

        const resultPath = path.join(dirPath, "result");
        const expectedFunctionsJSON = require(resultPath);
        fixLineBreaksInEachFunc(expectedFunctionsJSON);

        it(dirName, async() => {

            await db.query(ddl);
            
            const functions = await pgDriver.loadFunctions();
            const actualFunctionsJSON = functions.map(func => func.toJSON());

            assert.deepStrictEqual(
                actualFunctionsJSON,
                expectedFunctionsJSON
            );
        });
    }

    function fixLineBreaks(str) {
        return str.replace(/[\r\n]+/g, "\n");
    }

    function fixLineBreaksInEachFunc(funcs) {
        funcs.forEach(func => {
            if ( func ) {
                fixLineBreaksInFunc(func);
            }
        });
    }

    function fixLineBreaksInFunc(func) {
        if ( !func.parsed ) {
            return;
        }
        
        const body = func.parsed.body;
        if ( !body || !body.content ) {
            return;
        }

        body.content = fixLineBreaks(body.content);
    }
});