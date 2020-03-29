import {readDatabaseOptions} from "../utils";
import pg from "pg";
import {PgDBDriver} from "../../lib/db/PgDBDriver";
import assert from "assert";

describe("PgDBDriver", () => {
    const dbConfig = readDatabaseOptions();
    let db: pg.Client;

    beforeEach(async() => {
        db = new pg.Client(dbConfig);
        await db.connect();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);
    });

    afterEach(async() => {
        db.end();
    });

    it("load empty state", async() => {
        const pgDriver = new PgDBDriver(dbConfig);
        await pgDriver.connect();

        const functions = await pgDriver.loadFunctions();

        assert.equal(functions.length, 0);
    });

    it("load simple function", async() => {
        const body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func(id bigint)
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        const pgDriver = new PgDBDriver(dbConfig);
        await pgDriver.connect();

        const functions = await pgDriver.loadFunctions();

        assert.equal(functions.length, 1);
        assert.deepStrictEqual(functions[0].toJSON(), {
            createdByDDLManager: false,
            filePath: "(database)",
            identify: "public.test_func(id bigint)",
            name: "test_func",
            parsed: {
                schema: "public",
                name: "test_func",
                args: [
                    {
                        default: null,
                        in: null,
                        name: "id",
                        out: null,
                        type: "bigint"
                    }
                ],
                body: {
                    content: body
                },
                comment: null,
                cost: null,
                language: "plpgsql",
                parallel: null,
                returns: {
                    setof: null,
                    table: null,
                    type: "void"
                },
                immutable: null,
                returnsNullOnNull: null,
                stable: null,
                strict: null
            }
        });
    });

});