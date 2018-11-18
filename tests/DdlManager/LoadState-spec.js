"use strict";

//  env TEST_DB_ENV=rights32  mocha ./tests/ddl-manager/DDlManager/*-spec.js --exit

const assert = require("assert");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../DdlManager");

describe("DddlManager.loadState", () => {

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

});