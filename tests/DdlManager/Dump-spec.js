"use strict";

const assert = require("assert");
const fs = require("fs");
const del = require("del");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../lib/DdlManager");
const DDLCoach = require("../../lib/parser/DDLCoach");

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("DdlManager.dump", () => {
    let db;
    
    beforeEach(async() => {
        db = await getDbClient();

        await db.query(`
            drop schema public cascade;
            create schema public;

            drop schema if exists test cascade;
        `);

        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            del.sync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });

    afterEach(async() => {
        db.end();
    });

    it("dump nonexistent folder", async() => {
        try {
            await DdlManager.dump({
                db, 
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("dump empty db", async() => {

        await DdlManager.dump({
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

        await DdlManager.dump({
            db: {
                database: db.database,
                user: db.user,
                password: db.password,
                host: db.host,
                port: db.port
            }, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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

        await DdlManager.dump({
            db: {
                database: db.database,
                user: db.user,
                password: db.password,
                host: db.host,
                port: db.port
            }, 
            folder: ROOT_TMP_PATH
        });

        let sql;
        let content;

        sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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
        content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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
        let body = `
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

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/company/some_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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
        let body = `
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

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/company/some_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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
        let body = `
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

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/test/company/some_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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

    it("dump simple function and build, build should not replace freezed object", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;
        `);

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let filePath = ROOT_TMP_PATH + "/public/simple_func.sql";

        let sql = fs.readFileSync(filePath).toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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

        try {
            await DdlManager.build({
                db,
                folder: ROOT_TMP_PATH,
                throwError: true
            });
            throw new Error("expected error");
        } catch(err) {
            assert.equal(err.message, "cannot replace freeze function public.simple_func()");
        }
        
    });

    it("dump with unfreeze function", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;
        `);

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH,
            unfreeze: true
        });

        let filePath = ROOT_TMP_PATH + "/public/simple_func.sql";

        let sql = fs.readFileSync(filePath).toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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

        await DdlManager.build({
            db,
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        
        let result = await db.query("select simple_func() as test");
        assert.deepEqual(result.rows[0], {
            test: 2
        });
    });

    it("dump with unfreeze trigger", async() => {
        let body = `
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

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH,
            unfreeze: true
        });

        let filePath = ROOT_TMP_PATH + "/public/company/some_func.sql";

        let sql = fs.readFileSync(filePath).toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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

        await DdlManager.build({
            db,
            folder: ROOT_TMP_PATH,
            throwError: true
        });
        
        let result = await db.query("insert into company default values returning name");
        assert.deepEqual(result.rows[0], {
            name: "nice"
        });
    });

    it("dump simple function with comment, check comment in dumped file", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;

            comment on function public.simple_func() is $$test$$;
        `);

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
            functions: [{
                schema: "public",
                name: "simple_func",
                returns: {type: "integer"},
                language: "sql",
                args: [],
                body: "select 1"
            }],
            comments: [
                {
                    function: {
                        schema: "public",
                        name: "simple_func",
                        args: []
                    },
                    comment: "test"
                }
            ]
        });
    });

    
    it("dump simple function and trigger with comment, check comment in dumped file", async() => {
        let body = `
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
            comment on function some_func() is $$func$$;

            create trigger some_trigger
            after insert or update or delete
            on company
            for each row
            execute procedure some_func();
            comment on trigger some_trigger on company is $$trigger$$;
        `);

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/company/some_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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
            }],
            comments: [
                {
                    function: {
                        schema: "public",
                        name: "some_func",
                        args: []
                    },
                    comment: "func"
                },
                {
                    trigger: {
                        schema: "public",
                        table: "company",
                        name: "some_trigger"
                    },
                    comment: "trigger"
                }
            ]
        });
    });

    it("dump function with returns table", async() => {
        let body = `
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

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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
        let body = `
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

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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

    it("dump with unfreeze function with comment", async() => {
        await db.query(`
            create or replace function simple_func()
            returns integer as $$select 1$$
            language sql;

            comment on function simple_func() is $tag1$'$$nice\ncomment$tag1$;
        `);

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH,
            unfreeze: true
        });

        let filePath = ROOT_TMP_PATH + "/public/simple_func.sql";

        let sql = fs.readFileSync(filePath).toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
            functions: [{
                schema: "public",
                name: "simple_func",
                returns: {type: "integer"},
                language: "sql",
                args: [],
                body: "select 1"
            }],
            comments: [{
                function: {
                    schema: "public",
                    name: "simple_func",
                    args: []
                },
                comment: "'$$nice\ncomment"
            }]
        });

        let result = await db.query(`
            select
                pg_catalog.obj_description( pg_proc.oid ) as comment
            from information_schema.routines as routines

            left join pg_catalog.pg_proc as pg_proc on
                routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text

            where
                routines.routine_schema = 'public' and
                routines.routine_name = 'simple_func'
        `);

        assert.deepEqual(result.rows[0], {
            comment: "'$$nice\ncomment\nddl-manager-sync"
        });
    });

    it("dump with unfreeze trigger with comment", async() => {
        let body = `
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

            
            comment on trigger some_trigger on company is $tag1$'$$nice\ncomment$tag1$;
        `);

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH,
            unfreeze: true
        });

        let filePath = ROOT_TMP_PATH + "/public/company/some_func.sql";

        let sql = fs.readFileSync(filePath).toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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
            }],
            comments: [{
                trigger: {
                    schema: "public",
                    table: "company",
                    name: "some_trigger"
                },
                comment: "'$$nice\ncomment"
            }]
        });

        let result = await db.query(`
            select
                pg_catalog.obj_description( pg_trigger.oid ) as comment
            from pg_trigger
            where
                pg_trigger.tgname = 'some_trigger'
        `);

        assert.deepEqual(result.rows[0], {
            comment: "'$$nice\ncomment\nddl-manager-sync"
        });
    });

    it("dump two functions and trigger", async() => {
        let triggerBody = `
            begin
                PERFORM some_func( new.id );
                return new;
            end
        `;
        let funcBody = `
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

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/company/some_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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

    it("dump function two functions with same name and various comments, into one file", async() => {
        let body = `
            begin
            end
        `;
        await db.query(`
            create or replace function simple_func(x integer)
            returns void as $$${ body }$$
            language plpgsql;

            comment on function simple_func(integer) is 'x';

            create or replace function simple_func(y boolean)
            returns void as $$${ body }$$
            language plpgsql;

            comment on function simple_func(boolean) is 'y';
        `);

        await DdlManager.dump({
            db, 
            folder: ROOT_TMP_PATH
        });

        let sql = fs.readFileSync(ROOT_TMP_PATH + "/public/simple_func.sql").toString();
        let content = DDLCoach.parseSqlFile(sql);

        assert.deepEqual(content, {
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
                            name: "y",
                            type: "boolean"
                        }
                    ],
                    body
                }
            ],
            comments: [
                {
                    function: {
                        schema: "public",
                        name: "simple_func",
                        args: ["integer"]
                    },
                    comment: "x"
                },
                {
                    function: {
                        schema: "public",
                        name: "simple_func",
                        args: ["boolean"]
                    },
                    comment: "y"
                }
            ]
        });
    });

});