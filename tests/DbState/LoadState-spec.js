"use strict";

const assert = require("assert");
const fs = require("fs");
const del = require("del");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../lib/DdlManager");
const DbState = require("../../lib/DbState");

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("DbState.load", () => {
    let db;

    beforeEach(async() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            del.sync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);

        db = await getDbClient();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);
    });

    afterEach(async() => {
        db.end();
    });
    
    it("load empty state", async() => {
        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [],
            triggers: []
        });
    });

    it("load simple function", async() => {

        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func(id bigint)
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
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
                    returns: {type: "void"},
                    body
                }
            ],
            triggers: []
        });
    });

    it("load two functions", async() => {

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
            create function func_1(arg_1 bigint)
            returns integer as $body$${ body1 }$body$
            language plpgsql;

            create function func_2(arg_2 text)
            returns text as $body$${ body2 }$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
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
                    returns: {type: "integer"},
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
                    returns: {type: "text"},
                    body: body2
                }
            ],
            triggers: []
        });
    });

    it("load two functions with same name", async() => {
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
            create function func_1(arg_1 bigint)
            returns integer as $body$${ body1 }$body$
            language plpgsql;

            create function func_1(arg_1 text)
            returns integer as $body$${ body2 }$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
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
                    returns: {type: "integer"},
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
                    returns: {type: "integer"},
                    body: body2
                }
            ],
            triggers: []
        });
    });

    it("load two function without arguments", async() => {

        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func()
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: {type: "void"},
                    body
                }
            ],
            triggers: []
        });
    });

    
    it("load two function with returns table", async() => {
        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func()
            returns table(x text, y integer) as $body$${ body }$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
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
    });

    it("load two function with returns table and arguments", async() => {
        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func( nice text )
            returns table(x text, y integer) as $body$${ body }$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
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
    });

    it("load simple function with arg default", async() => {
        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func(id bigint default 1)
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
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
                    returns: {type: "void"},
                    body
                }
            ],
            triggers: []
        });
    });

    it("load simple function with arg default null", async() => {
        let body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func(id bigint default null)
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
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
                            default: "null ::bigint"
                        }
                    ],
                    returns: {type: "void"},
                    body
                }
            ],
            triggers: []
        });
    });

    it("load trigger", async() => {
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

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: {type: "trigger"},
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
    });

    it("load trigger with when condition", async() => {
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

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: {type: "trigger"},
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
    });


    it("load simple function, created by DdlManager.build", async() => {

        let folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);


        let body = `
            begin
                raise notice 'test';
            end
        `;
        
        fs.writeFileSync(folderPath + "/test1.sql", `
            create or replace function test_func(id bigint)
            returns void as $body$${ body }$body$
            language plpgsql
        `);

        await DdlManager.build({
            db, 
            folder: folderPath
        });

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
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
                    returns: {type: "void"},
                    body
                }
            ],
            triggers: []
        });
    });

    
    it("load trigger, created by DdlManager.build", async() => {

        let folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        let body = `
            begin
                return new;
            end
        `;
        await db.query(`
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

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [
                {
                    language: "plpgsql",
                    freeze: false,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: {type: "trigger"},
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
    });

    it("ignore aggregate functions", async() => {

        await db.query(`
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

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [],
            triggers: []
        });
    });

    it("ignore constraint triggers", async() => {

        await db.query(`
            create table test (
                id serial primary key
            );

            create table test_units (
                id serial primary key,
                id_parent integer not null
                    references test
                        on update cascade
                        on delete cascade
            );
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [],
            triggers: []
        });
    });

    it("load simple function, language sql", async() => {
        
        await db.query(`
            create function test_func_sql()
            returns integer as $body$select 1$body$
            language sql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [
                {
                    language: "sql",
                    freeze: true,
                    schema: "public",
                    name: "test_func_sql",
                    args: [],
                    returns: {type: "integer"},
                    body: "select 1"
                }
            ],
            triggers: []
        });
    });

    it("load simple function, returns schema.table", async() => {
        
        await db.query(`
            create table company (
                id serial primary key
            );

            create function some_func()
            returns public.company as $body$begin\nend$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "some_func",
                    args: [],
                    returns: {type: "public.company"},
                    body: "begin\nend"
                }
            ],
            triggers: []
        });
    });

    it("load simple function, returns setof schema.table", async() => {
        
        await db.query(`
            create table company (
                id serial primary key
            );

            create function some_func()
            returns setof public.company as $body$begin\nend$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "some_func",
                    args: [],
                    returns: {
                        setof: true,
                        type: "public.company"
                    },
                    body: "begin\nend"
                }
            ],
            triggers: []
        });
    });

    it("load simple function, arg table", async() => {
        
        await db.query(`
            create table company (
                id serial primary key
            );

            create function some_func(company public.company)
            returns void as $body$begin\nend$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "some_func",
                    args: [
                        {
                            name: "company",
                            type: "public.company"
                        }
                    ],
                    returns: {type: "void"},
                    body: "begin\nend"
                }
            ],
            triggers: []
        });
    });

    it("load simple function, arg without name", async() => {
        
        await db.query(`
            create function some_func(text)
            returns void as $body$begin\nend$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "some_func",
                    args: [
                        {
                            name: false,
                            type: "text"
                        }
                    ],
                    returns: {type: "void"},
                    body: "begin\nend"
                }
            ],
            triggers: []
        });
    });

    it("load simple function, in/out arg", async() => {
        
        await db.query(`
            create function some_func(in id integer, out name text)
            returns text as $body$begin\nend$body$
            language plpgsql;
        `);

        let state = new DbState(db);
        await state.load();

        assert.deepEqual(state.toJSON(), {
            functions: [
                {
                    language: "plpgsql",
                    freeze: true,
                    schema: "public",
                    name: "some_func",
                    args: [
                        {
                            name: "id",
                            type: "integer"
                        },
                        {
                            out: true,
                            name: "name",
                            type: "text"
                        }
                    ],
                    returns: {type: "text"},
                    body: "begin\nend"
                }
            ],
            triggers: []
        });
    });

});