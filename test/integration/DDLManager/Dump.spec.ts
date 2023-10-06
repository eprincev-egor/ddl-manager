import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../getDbClient";
import { DDLManager } from "../../../lib/DDLManager";
import { FileParser } from "../../../lib/parser/FileParser";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { DatabaseTrigger } from "../../../lib/database/schema/DatabaseTrigger";


use(chaiShallowDeepEqualPlugin);

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/DDLManager.dump", () => {
    let db: any;
    const dbConfig = require("../../../ddl-manager-config");

    beforeEach(async() => {
        db = await getDBClient();

        await db.query(`
            drop schema public cascade;
            create schema public;

            drop schema if exists test cascade;
        `);

        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });

    afterEach(async() => {
        await db.end();
    });

    it("dump nonexistent folder", async() => {
        try {
            await DDLManager.dump({
                db, 
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("dump empty db", async() => {

        await DDLManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        // expected dump without errors
        assert.ok(true);
    });

    it("dump simple function", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;
        `);

        await DDLManager.dump({
            db: {
                database: dbConfig.database,
                user: dbConfig.user,
                password: dbConfig.password,
                host: dbConfig.host,
                port: dbConfig.port
            }, 
            folder: ROOT_TMP_PATH
        });

        const sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        const content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [{
                schema: "public",
                name: "simple_func",
                returns: {type: "integer"},
                language: "sql",
                args: [],
                body: "select 1"
            }]
        });
    });

    it("dump simple function from some schema", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;

            create schema test;

            create or replace function test.simple_func()
            returns integer as $$select 1$$
            language sql;
        `);

        await DDLManager.dump({
            db: {
                database: dbConfig.database,
                user: dbConfig.user,
                password: dbConfig.password,
                host: dbConfig.host,
                port: dbConfig.port
            }, 
            folder: ROOT_TMP_PATH
        });

        let sql;
        let content;

        sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [{
                schema: "public",
                name: "simple_func",
                returns: {type: "integer"},
                language: "sql",
                args: [],
                body: "select 1"
            }]
        });

        sql = fs.readFileSync(ROOT_TMP_PATH + "/test/simple_func.sql").toString();
        content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [{
                schema: "test",
                name: "simple_func",
                returns: {type: "integer"},
                language: "sql",
                args: [],
                body: "select 1"
            }]
        });
    });

    it("dump simple function and trigger", async() => {
        const body = `
            begin
                return new;
            end
        `;
        await db.query(`
            create table company (
                id serial primary key
            );

            create or replace function some_func()
            returns trigger as $body$${ body }$body$
            language plpgsql;

            create trigger some_trigger
            after insert or update or delete
            on company
            for each row
            execute procedure some_func();
        `);

        await DDLManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        const sql = fs.readFileSync(ROOT_TMP_PATH + "/public/company/some_func.sql").toString();
        const content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [{
                schema: "public",
                name: "some_func",
                returns: {type: "trigger"},
                language: "plpgsql",
                args: [],
                body
            }],
            triggers: [{
                table: {
                    schema: "public",
                    name: "company"
                },
                after: true,
                insert: true,
                update: true,
                delete: true,
                name: "some_trigger",
                procedure: {
                    schema: "public",
                    name: "some_func"
                }
            }]
        });
    });

    
    it("dump simple function and two triggers", async() => {
        const body = `
            begin
                return new;
            end
        `;
        await db.query(`
            create table company (
                id serial primary key
            );

            create or replace function some_func()
            returns trigger as $body$${ body }$body$
            language plpgsql;

            create trigger some_trigger
            after insert
            on company
            for each row
            execute procedure some_func();

            create trigger some_trigger2
            after update
            on company
            for each row
            execute procedure some_func();
        `);

        await DDLManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        const sql = fs.readFileSync(ROOT_TMP_PATH + "/public/company/some_func.sql").toString();
        const content = FileParser.parse(sql) as any;

        content.triggers.sort((triggerA: DatabaseTrigger, triggerB: DatabaseTrigger) =>
            triggerA.name < triggerB.name ? -1 : 1
        );

        expect(content).to.be.shallowDeepEqual({
            functions: [{
                schema: "public",
                name: "some_func",
                returns: {type: "trigger"},
                language: "plpgsql",
                args: [],
                body
            }],
            triggers: [
                {
                    table: {
                        schema: "public",
                        name: "company"
                    },
                    after: true,
                    insert: true,
                    name: "some_trigger",
                    procedure: {
                        schema: "public",
                        name: "some_func"
                    }
                },
                {
                    table: {
                        schema: "public",
                        name: "company"
                    },
                    after: true,
                    update: true,
                    name: "some_trigger2",
                    procedure: {
                        schema: "public",
                        name: "some_func"
                    }
                }
            ]
        });
    });

    it("dump function from public and trigger table from test schema, file must be in folder test/company", async() => {
        const body = `
            begin
                return new;
            end
        `;
        await db.query(`
            create schema test;

            create table test.company (
                id serial primary key
            );

            create or replace function public.some_func()
            returns trigger as $body$${ body }$body$
            language plpgsql;

            create trigger some_trigger
            after insert or update or delete
            on test.company
            for each row
            execute procedure some_func();
        `);

        await DDLManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        const sql = fs.readFileSync(ROOT_TMP_PATH + "/test/company/some_func.sql").toString();
        const content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [{
                schema: "public",
                name: "some_func",
                returns: {type: "trigger"},
                language: "plpgsql",
                args: [],
                body
            }],
            triggers: [{
                table: {
                    schema: "test",
                    name: "company"
                },
                after: true,
                insert: true,
                update: true,
                delete: true,
                name: "some_trigger",
                procedure: {
                    schema: "public",
                    name: "some_func"
                }
            }]
        });
    });

    it("dump simple function and build, build can replace frozen object", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;
        `);

        await DDLManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        const filePath = ROOT_TMP_PATH + "/public/simple_func.sql";

        const sql = fs.readFileSync(filePath).toString();
        const content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [{
                schema: "public",
                name: "simple_func",
                returns: {type: "integer"},
                language: "sql",
                args: [],
                body: "select 1"
            }]
        });

        fs.writeFileSync(filePath, `
            create or replace function simple_func()
            returns integer as $$select 2$$
            language sql;
        `);

        await DDLManager.build({
            db,
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        
        const result = await db.query("select simple_func() as simple_func");
        expect(result.rows[0]).to.be.shallowDeepEqual({
            simple_func: 2
        });
    });

    it("dump with unfreeze function", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;
        `);

        await DDLManager.dump({
            db, 
            folder: ROOT_TMP_PATH,
            unfreeze: true
        });

        const filePath = ROOT_TMP_PATH + "/public/simple_func.sql";

        const sql = fs.readFileSync(filePath).toString();
        const content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [{
                schema: "public",
                name: "simple_func",
                returns: {type: "integer"},
                language: "sql",
                args: [],
                body: "select 1"
            }]
        });

        fs.writeFileSync(filePath, `
            create or replace function simple_func()
            returns integer as $$select 2$$
            language sql;
        `);

        await DDLManager.build({
            db,
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        
        const result = await db.query("select simple_func() as test");
        expect(result.rows[0]).to.be.shallowDeepEqual({
            test: 2
        });
    });

    it("dump with unfreeze trigger", async() => {
        const body = `
            begin
                return new;
            end
        `;
        await db.query(`
            create table company (
                id serial primary key,
                name text
            );

            create or replace function some_func()
            returns trigger as $body$${ body }$body$
            language plpgsql;

            create trigger some_trigger
            after insert or update or delete
            on company
            for each row
            execute procedure some_func();
        `);

        await DDLManager.dump({
            db, 
            folder: ROOT_TMP_PATH,
            unfreeze: true
        });

        const filePath = ROOT_TMP_PATH + "/public/company/some_func.sql";

        const sql = fs.readFileSync(filePath).toString();
        const content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [{
                schema: "public",
                name: "some_func",
                returns: {type: "trigger"},
                language: "plpgsql",
                args: [],
                body
            }],
            triggers: [{
                table: {
                    schema: "public",
                    name: "company"
                },
                after: true,
                insert: true,
                update: true,
                delete: true,
                name: "some_trigger",
                procedure: {
                    schema: "public",
                    name: "some_func"
                }
            }]
        });

        fs.writeFileSync(filePath, `
            create or replace function some_func()
            returns trigger as $$
            begin
                new.name = 'nice';
                return new;
            end
            $$
            language plpgsql;

            create trigger some_trigger
            before insert
            on company
            for each row
            execute procedure some_func();
        `);

        await DDLManager.build({
            db,
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        
        const result = await db.query("insert into company default values returning name");
        expect(result.rows[0]).to.be.shallowDeepEqual({
            name: "nice"
        });
    });

    it("dump function with returns table", async() => {
        const body = `
            begin
                id = 1;
                name = 'nice';
                return next;
            end
        `;
        await db.query(`
            create or replace function simple_func()
            returns table(id integer, name text) as $$${ body }$$
            language plpgsql;
        `);

        await DDLManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        const sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        const content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [{
                schema: "public",
                name: "simple_func",
                returns: {
                    table: [
                        {
                            name: "id",
                            type: "integer"
                        },
                        {
                            name: "name",
                            type: "text"
                        }
                    ]
                },
                language: "plpgsql",
                args: [],
                body
            }]
        });
    });

    
    it("dump function two functions with same name into one file", async() => {
        const body = `
            begin
            end
        `;
        await db.query(`
            create or replace function simple_func(x integer)
            returns void as $$${ body }$$
            language plpgsql;

            create or replace function simple_func(x boolean)
            returns void as $$${ body }$$
            language plpgsql;
        `);

        await DDLManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        const sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        const content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [
                {
                    schema: "public",
                    name: "simple_func",
                    returns: {
                        type: "void"
                    },
                    language: "plpgsql",
                    args: [
                        {
                            name: "x",
                            type: "integer"
                        }
                    ],
                    body
                },
                {
                    schema: "public",
                    name: "simple_func",
                    returns: {
                        type: "void"
                    },
                    language: "plpgsql",
                    args: [
                        {
                            name: "x",
                            type: "boolean"
                        }
                    ],
                    body
                }
            ]
        });
    });

    it("dump two functions and trigger", async() => {
        const triggerBody = `
            begin
                PERFORM some_func( new.id );
                return new;
            end
        `;
        const funcBody = `
            begin
                update company set
                    id = id + 1
                where id = some_id;
            end
        `;
        await db.query(`
            create table company (
                id serial primary key
            );

            create or replace function some_func(some_id integer)
            returns void as $body$${ funcBody }$body$
            language plpgsql;

            create or replace function some_func()
            returns trigger as $body$${ triggerBody }$body$
            language plpgsql;

            create trigger some_trigger
            after insert or update or delete
            on company
            for each row
            execute procedure some_func();
        `);

        await DDLManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        const sql = fs.readFileSync(ROOT_TMP_PATH + "/public/company/some_func.sql").toString();
        const content = FileParser.parse(sql) as any;

        expect(content).to.be.shallowDeepEqual({
            functions: [
                {
                    schema: "public",
                    name: "some_func",
                    returns: {type: "void"},
                    language: "plpgsql",
                    args: [{
                        name: "some_id",
                        type: "integer"
                    }],
                    body: funcBody
                },
                {
                    schema: "public",
                    name: "some_func",
                    returns: {type: "trigger"},
                    language: "plpgsql",
                    args: [],
                    body: triggerBody
                }
            ],
            triggers: [{
                table: {
                    schema: "public",
                    name: "company"
                },
                after: true,
                insert: true,
                update: true,
                delete: true,
                name: "some_trigger",
                procedure: {
                    schema: "public",
                    name: "some_func"
                }
            }]
        });
    });
    
    it("dump simple function, file should end with ;", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;
        `);

        await DDLManager.dump({
            db: {
                database: dbConfig.database,
                user: dbConfig.user,
                password: dbConfig.password,
                host: dbConfig.host,
                port: dbConfig.port
            }, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        sql = sql.trim();

        assert.ok(
            /;$/.test(sql)
        );
    });


});