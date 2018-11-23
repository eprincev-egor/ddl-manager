"use strict";

const assert = require("assert");
const fs = require("fs");
const del = require("del");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../lib/DdlManager");

const ROOT_TMP_PATH = __dirname + "/tmp";

before(() => {
    if ( !fs.existsSync(ROOT_TMP_PATH) ) {
        fs.mkdirSync(ROOT_TMP_PATH);
    }
});

describe("DdlManager.loadState", () => {

    it("load empty state", async() => {
        let db = await getDbClient();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

        let state = await DdlManager.loadState(db);

        assert.deepEqual(state, {
            functions: [],
            triggers: []
        });

        db.end();
    });

    it("load simple function", async() => {
        let db = await getDbClient();

        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;

            create function test_func(id bigint)
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "test_func",
                    args: [
                        {
                            name: "id",
                            type: "bigint"
                        }
                    ],
                    returns: "void",
                    body
                }
            ],
            triggers: []
        });

        db.end();
    });

    it("load two functions", async() => {
        let db = await getDbClient();

        let body1 = `
            begin
                raise notice 'test 1';
            end
        `;
        let body2 = `
            begin
                raise notice 'test 2';
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;

            create function func_1(arg_1 bigint)
            returns integer as $body$${ body1 }$body$
            language plpgsql;

            create function func_2(arg_2 text)
            returns text as $body$${ body2 }$body$
            language plpgsql;
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "func_1",
                    args: [
                        {
                            name: "arg_1",
                            type: "bigint"
                        }
                    ],
                    returns: "integer",
                    body: body1
                },
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "func_2",
                    args: [
                        {
                            name: "arg_2",
                            type: "text"
                        }
                    ],
                    returns: "text",
                    body: body2
                }
            ],
            triggers: []
        });

        db.end();
    });

    it("load two functions with same name", async() => {
        let db = await getDbClient();

        let body1 = `
            begin
                raise notice 'test 1';
            end
        `;
        let body2 = `
            begin
                raise notice 'test 2';
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;

            create function func_1(arg_1 bigint)
            returns integer as $body$${ body1 }$body$
            language plpgsql;

            create function func_1(arg_1 text)
            returns integer as $body$${ body2 }$body$
            language plpgsql;
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "func_1",
                    args: [
                        {
                            name: "arg_1",
                            type: "bigint"
                        }
                    ],
                    returns: "integer",
                    body: body1
                },
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "func_1",
                    args: [
                        {
                            name: "arg_1",
                            type: "text"
                        }
                    ],
                    returns: "integer",
                    body: body2
                }
            ],
            triggers: []
        });

        db.end();
    });

    it("load two function without arguments", async() => {
        let db = await getDbClient();

        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;

            create function test_func()
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: "void",
                    body
                }
            ],
            triggers: []
        });

        db.end();
    });

    
    it("load two function with returns table", async() => {
        let db = await getDbClient();

        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;

            create function test_func()
            returns table(x text, y integer) as $body$${ body }$body$
            language plpgsql;
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: {table: [
                        {
                            name: "x",
                            type: "text"
                        },
                        {
                            name: "y",
                            type: "integer"
                        }
                    ]},
                    body
                }
            ],
            triggers: []
        });

        db.end();
    });

    it("load two function with returns table and arguments", async() => {
        let db = await getDbClient();

        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;

            create function test_func( nice text )
            returns table(x text, y integer) as $body$${ body }$body$
            language plpgsql;
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "test_func",
                    args: [
                        {
                            name: "nice",
                            type: "text"
                        }
                    ],
                    returns: {table: [
                        {
                            name: "x",
                            type: "text"
                        },
                        {
                            name: "y",
                            type: "integer"
                        }
                    ]},
                    body
                }
            ],
            triggers: []
        });

        db.end();
    });

    it("load simple function with arg default", async() => {
        let db = await getDbClient();

        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;

            create function test_func(id bigint default 1)
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "test_func",
                    args: [
                        {
                            name: "id",
                            type: "bigint",
                            default: "1"
                        }
                    ],
                    returns: "void",
                    body
                }
            ],
            triggers: []
        });

        db.end();
    });

    it("load trigger", async() => {
        let db = await getDbClient();

        let body = `
            begin
                return new;
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;
            
            create table test (
                name text,
                note text
            );

            create function test_func()
            returns trigger as $body$${ body }$body$
            language plpgsql;

            create trigger test_trigger
            after insert or update of name, note or delete
            on test
            for each row
            execute procedure test_func();
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: "trigger",
                    body
                }
            ],
            triggers: [
                {
                    freeze: true,
                    table: {
                        schema: "public",
                        name: "test"
                    },
                    name: "test_trigger",
                    after: true,
                    insert: true,
                    update: ["name", "note"],
                    delete: true,
                    procedure: {
                        schema: "public",
                        name: "test_func"
                    }
                }
            ]
        });

        db.end();
    });

    it("load trigger with when condition", async() => {
        let db = await getDbClient();

        let body = `
            begin
                return new;
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;
            
            create table test (
                name text,
                note text
            );

            create function test_func()
            returns trigger as $body$${ body }$body$
            language plpgsql;

            create trigger test_trigger
            after insert or update of name, note or delete
            on test
            for each row
            when (pg_trigger_depth() = 0)
            execute procedure test_func();
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: "trigger",
                    body
                }
            ],
            triggers: [
                {
                    freeze: true,
                    table: {
                        schema: "public",
                        name: "test"
                    },
                    name: "test_trigger",
                    after: true,
                    insert: true,
                    update: ["name", "note"],
                    delete: true,
                    when: "pg_trigger_depth() = 0",
                    procedure: {
                        schema: "public",
                        name: "test_func"
                    }
                }
            ]
        });

        db.end();
    });


    it("load simple function, created by DdlManager.build", async() => {
        let db = await getDbClient();
        let folderPath = ROOT_TMP_PATH + "/simple-func";
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);


        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

        fs.writeFileSync(folderPath + "/test1.sql", `
            create or replace function test_func(id bigint)
            returns void as $body$${ body }$body$
            language plpgsql
        `);

        await DdlManager.build({
            db, 
            folder: folderPath
        });

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: false,
                    schema: "public",
                    name: "test_func",
                    args: [
                        {
                            name: "id",
                            type: "bigint"
                        }
                    ],
                    returns: "void",
                    body
                }
            ],
            triggers: []
        });

        db.end();
    });

    
    it("load trigger, created by DdlManager.build", async() => {
        let db = await getDbClient();
        let folderPath = ROOT_TMP_PATH + "/simple-func";
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);

        let body = `
            begin
                return new;
            end
        `;
        await db.query(`
            drop schema public cascade;
            create schema public;
            
            create table test (
                name text,
                note text
            );
        `);

        fs.writeFileSync(folderPath + "/test2.sql", `
            create or replace function test_func()
            returns trigger as $body$${ body }$body$
            language plpgsql;

            create trigger test_trigger
            after insert or update of name, note or delete
            on test
            for each row
            execute procedure test_func();
        `);

        await DdlManager.build({
            db, 
            folder: folderPath
        });

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "plpgsql",
                    freeze: false,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: "trigger",
                    body
                }
            ],
            triggers: [
                {
                    freeze: false,
                    table: {
                        schema: "public",
                        name: "test"
                    },
                    after: true,
                    insert: true,
                    name: "test_trigger",
                    update: ["name", "note"],
                    delete: true,
                    procedure: {
                        schema: "public",
                        name: "test_func"
                    }
                }
            ]
        });

        db.end();
    });

    it("ignore aggregate functions", async() => {
        let db = await getDbClient();

        await db.query(`
            drop schema public cascade;
            create schema public;

            CREATE FUNCTION first_agg(anyelement, anyelement) RETURNS anyelement
                LANGUAGE sql IMMUTABLE STRICT
            AS $_$
                    SELECT $1;
            $_$;

            CREATE AGGREGATE first(anyelement) (
                SFUNC = first_agg,
                STYPE = anyelement
            );
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [],
            triggers: []
        });

        db.end();
    });

    it("load simple function, language sql", async() => {
        let db = await getDbClient();

        await db.query(`
            drop schema public cascade;
            create schema public;

            create function test_func_sql()
            returns integer as $body$select 1$body$
            language sql;
        `);

        let state = await DdlManager.loadState(db);
        assert.deepEqual(state, {
            functions: [
                {
                    language: "sql",
                    freeze: true,
                    schema: "public",
                    name: "test_func_sql",
                    args: [],
                    returns: "integer",
                    body: "select 1"
                }
            ],
            triggers: []
        });

        db.end();
    });

});