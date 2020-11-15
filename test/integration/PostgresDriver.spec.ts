import fs from "fs";
import fse from "fs-extra";
import assert from "assert";
import { getDBClient } from "./getDbClient";
import { DDLManager } from "../../lib/DDLManager";
import { PostgresDriver } from "../../lib/database/PostgresDriver";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { From, Select, SelectColumn, Table, TableReference, Expression, ColumnReference, Operator } from "../../lib/ast";

use(chaiShallowDeepEqualPlugin);

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/PostgresDriver.loadState", () => {
    let db: any;

    beforeEach(async() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);

        db = await getDBClient();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);
    });

    afterEach(async() => {
        db.end();
    });

    async function loadState() {
        const driver = new PostgresDriver(db);
        const state = await driver.loadState();
        return state;
    }

    it("load empty state", async() => {
        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [],
            triggers: []
        });
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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

        const body1 = `
            begin
                raise notice 'test 1';
            end
        `;
        const body2 = `
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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
                    frozen: true,
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
        const body1 = `
            begin
                raise notice 'test 1';
            end
        `;
        const body2 = `
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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
                    frozen: true,
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

        const body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func()
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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
        const body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func()
            returns table(x text, y integer) as $body$${ body }$body$
            language plpgsql;
        `);

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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
        const body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func( nice text )
            returns table(x text, y integer) as $body$${ body }$body$
            language plpgsql;
        `);

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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
        const body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func(id bigint default 1)
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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
        const body = `
            begin
                raise notice 'test';
            end
        `;
        await db.query(`
            create function test_func(id bigint default null)
            returns void as $body$${ body }$body$
            language plpgsql;
        `);

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
                    schema: "public",
                    name: "test_func",
                    args: [
                        {
                            name: "id",
                            type: "bigint",
                            default: "null :: bigint"
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
        const body = `
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: {type: "trigger"},
                    body
                }
            ],
            triggers: [
                {
                    frozen: true,
                    table: {
                        schema: "public",
                        name: "test"
                    },
                    name: "test_trigger",
                    after: true,
                    insert: true,
                    updateOf: ["name", "note"],
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
        const body = `
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: {type: "trigger"},
                    body
                }
            ],
            triggers: [
                {
                    frozen: true,
                    table: {
                        schema: "public",
                        name: "test"
                    },
                    name: "test_trigger",
                    after: true,
                    insert: true,
                    updateOf: ["name", "note"],
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


    it("load simple function, created by DDLManager.build", async() => {

        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);


        const body = `
            begin
                raise notice 'test';
            end
        `;
        
        fs.writeFileSync(folderPath + "/test1.sql", `
            create or replace function test_func(id bigint)
            returns void as $body$${ body }$body$
            language plpgsql
        `);

        await DDLManager.build({
            db, 
            folder: folderPath
        });

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: false,
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

    
    it("load trigger, created by DDLManager.build", async() => {

        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        const body = `
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

        await DDLManager.build({
            db, 
            folder: folderPath
        });

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: false,
                    schema: "public",
                    name: "test_func",
                    args: [],
                    returns: {type: "trigger"},
                    body
                }
            ],
            triggers: [
                {
                    frozen: false,
                    table: {
                        schema: "public",
                        name: "test"
                    },
                    after: true,
                    insert: true,
                    name: "test_trigger",
                    updateOf: ["name", "note"],
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "sql",
                    frozen: true,
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
                    schema: "public",
                    name: "some_func",
                    args: [
                        {
                            name: null,
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

        const state = await loadState();

        expect(state).to.be.shallowDeepEqual({
            functions: [
                {
                    language: "plpgsql",
                    frozen: true,
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

    it("updateCachePackage", async() => {
        
        await db.query(`
            create table companies (
                id serial primary key,
                name text not null unique,
                orders_profit numeric default 0
            );
            create table orders (
                id serial primary key,
                id_client integer not null,
                profit numeric
            );

            insert into companies
                (name)
            select
                'c' || i
            from generate_series(1, 10) as i;

            insert into orders
                (id_client, profit)
            select
                company_id, 100
            from generate_series(1, 10) as company_id;
        `);

        const driver = new PostgresDriver(db);
        
        const companiesTable = new Table(
            "public",
            "companies"
        );
        const companiesTableRef = new TableReference( companiesTable );

        const ordersTable = new Table(
            "public",
            "orders"
        );
        const ordersTableRef = new TableReference( ordersTable );

        const select = new Select({
            columns: [
                new SelectColumn({
                    expression: Expression.funcCall("sum", [
                        new Expression([
                            new ColumnReference(ordersTableRef, "profit")
                        ])
                    ]),
                    name: "orders_profit"
                })
            ],
            from: [
                new From( ordersTableRef )
            ],
            where: new Expression([
                new ColumnReference(ordersTableRef, "id_client"),
                new Operator("="),
                new ColumnReference(companiesTableRef, "id")
            ])
        });

        let result: any;
        let updatedCount: any;

        updatedCount = await driver.updateCachePackage(
            select,
            companiesTableRef,
            3
        );

        assert.strictEqual(updatedCount, 3);

        result = await db.query("select * from companies order by id");
        assert.deepStrictEqual(result.rows, [
            {id: 1, name: "c1", orders_profit: "100"},
            {id: 2, name: "c2", orders_profit: "100"},
            {id: 3, name: "c3", orders_profit: "100"},
            {id: 4, name: "c4", orders_profit: "0"},
            {id: 5, name: "c5", orders_profit: "0"},
            {id: 6, name: "c6", orders_profit: "0"},
            {id: 7, name: "c7", orders_profit: "0"},
            {id: 8, name: "c8", orders_profit: "0"},
            {id: 9, name: "c9", orders_profit: "0"},
            {id:10, name:"c10", orders_profit: "0"},
        ], "first update");

        // after second update, more rows should be updated
        // also should be update broken rows
        await db.query("update companies set orders_profit = 20 where id in (1,2,3)");
        updatedCount = await driver.updateCachePackage(
            select,
            companiesTableRef,
            6
        );

        assert.strictEqual(updatedCount, 6);

        result = await db.query("select * from companies order by id");
        assert.deepStrictEqual(result.rows, [
            {id: 1, name: "c1", orders_profit: "100"},
            {id: 2, name: "c2", orders_profit: "100"},
            {id: 3, name: "c3", orders_profit: "100"},
            {id: 4, name: "c4", orders_profit: "100"},
            {id: 5, name: "c5", orders_profit: "100"},
            {id: 6, name: "c6", orders_profit: "100"},
            {id: 7, name: "c7", orders_profit: "0"},
            {id: 8, name: "c8", orders_profit: "0"},
            {id: 9, name: "c9", orders_profit: "0"},
            {id:10, name:"c10", orders_profit: "0"},
        ], "second update");

        
        // last update, all rows should be updated
        updatedCount = await driver.updateCachePackage(
            select,
            companiesTableRef,
            5
        );

        assert.strictEqual(updatedCount, 4);

        result = await db.query("select * from companies order by id");
        assert.deepStrictEqual(result.rows, [
            {id: 1, name: "c1", orders_profit: "100"},
            {id: 2, name: "c2", orders_profit: "100"},
            {id: 3, name: "c3", orders_profit: "100"},
            {id: 4, name: "c4", orders_profit: "100"},
            {id: 5, name: "c5", orders_profit: "100"},
            {id: 6, name: "c6", orders_profit: "100"},
            {id: 7, name: "c7", orders_profit: "100"},
            {id: 8, name: "c8", orders_profit: "100"},
            {id: 9, name: "c9", orders_profit: "100"},
            {id:10, name:"c10", orders_profit: "100"},
        ], "third update");
    });

});