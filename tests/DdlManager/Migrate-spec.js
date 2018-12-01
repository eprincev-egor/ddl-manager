"use strict";

const assert = require("assert");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../lib/DdlManager");

describe("DlManager.migrate", () => {
    let db;
    
    beforeEach(async() => {
        db = await getDbClient();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);
    });

    afterEach(async() => {
        db.end();
    });

    it("migrate null", async() => {

        try {
            await DdlManager.migrate(null, null);
            
            assert.ok(false, "expected error for null");
        } catch(err) {
            assert.equal(err.message, "invalid diff");
        }
    });

    it("migrate simple function", async() => {

        let rnd = Math.round( 10000 * Math.random() );
        
        await DdlManager.migrate(db, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {
                        language: "plpgsql",
                        schema: "public",
                        name: "test_migrate_function",
                        args: [],
                        returns: {type: "bigint"},
                        body: `begin
                            return ${ rnd };
                        end`
                    }
                ],
                triggers: []
            }
        });

        let result = await db.query("select test_migrate_function()");
        let row = result && result.rows[0];
        
        result = row.test_migrate_function;

        assert.equal(result, rnd);
    });

    it("migrate function and trigger", async() => {

        await db.query(`
            create table ddl_manager_test (
                name text,
                note text
            );
        `);

        await DdlManager.migrate(db, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {
                        language: "plpgsql",
                        schema: "public",
                        name: "some_action_on_diu_test",
                        args: [],
                        returns: {type: "trigger"},
                        body: `begin
                            raise exception 'success';
                        end`
                    }
                ],
                triggers: [
                    {
                        table: {
                            schema: "public",
                            name: "ddl_manager_test"
                        },
                        after: true,
                        insert: true,
                        update: ["name", "note"],
                        delete: true,
                        name: "some_action_on_diu_test_trigger",
                        procedure: {
                            schema: "public",
                            name: "some_action_on_diu_test"
                        }
                    }
                ]
            }
        });

        // check trigger on table
        try {
            await db.query(`
                insert into ddl_manager_test
                default values
            `);

            assert.ok(false, "expected special error from trigger");
        } catch(err) {
            assert.equal(err.message, "success");
        }
    });

    it("twice migrate function and trigger", async() => {

        await db.query(`
            create table ddl_manager_test (
                name text,
                note text
            );
        `);

        let diff = {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {
                        language: "plpgsql",
                        schema: "public",
                        name: "some_action_on_diu_test",
                        args: [],
                        returns: {type: "trigger"},
                        body: `begin
                            raise exception 'success';
                        end`
                    }
                ],
                triggers: [
                    {
                        table: {
                            schema: "public",
                            name: "ddl_manager_test"
                        },
                        after: true,
                        insert: true,
                        update: ["name", "note"],
                        delete: true,
                        name: "some_action_on_diu_test_trigger",
                        procedure: {
                            schema: "public",
                            name: "some_action_on_diu_test"
                        }
                    }
                ]
            }
        };

        // do it twice without errors
        await DdlManager.migrate(db, diff);
        await DdlManager.migrate(db, diff);
        

        // check trigger on table
        try {
            await db.query(`
                insert into ddl_manager_test
                default values
            `);

            assert.ok(false, "expected special error from trigger");
        } catch(err) {
            assert.equal(err.message, "success");
        }
        
    });

    it("error on replace freeze function", async() => {
        await db.query(`
            create function test()
            returns integer as $$select 1$$
            language sql;
        `);

        let result = await DdlManager.migrate(db, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {
                        language: "sql",
                        schema: "public",
                        name: "test",
                        args: [],
                        returns: {type: "integer"},
                        body: "select 2"
                    }
                ],
                triggers: []
            }
        });

        assert.equal(result.errors.length, 1);
        let err = result.errors[0];
        assert.equal(err.message, "cannot replace freeze function public.test()");
    });

    it("error on drop freeze function", async() => {
        await db.query(`
            create function test()
            returns integer as $$select 1$$
            language sql;
        `);

        let result = await DdlManager.migrate(db, {
            drop: {
                functions: [
                    {
                        language: "sql",
                        schema: "public",
                        name: "test",
                        args: [],
                        returns: {type: "integer"},
                        body: "select 2"
                    }
                ],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        });

        assert.equal(result.errors.length, 1);
        let err = result.errors[0];
        assert.equal(err.message, "cannot drop freeze function public.test()");
    });

    it("freeze function with another args", async() => {
        await db.query(`
            create function test(a integer)
            returns integer as $$select 1$$
            language sql;
        `);

        await DdlManager.migrate(db, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {
                        language: "sql",
                        schema: "public",
                        name: "test",
                        args: [
                            {
                                name: "a",
                                type: "integer"
                            },
                            {
                                name: "b",
                                type: "integer"
                            }
                        ],
                        returns: {type: "integer"},
                        body: "select 2"
                    }
                ],
                triggers: []
            }
        });

        let result = await db.query("select test(1, 2)");
        let row = result && result.rows[0];
        
        result = row.test;

        assert.equal(result, 2);

    });

    
    it("freeze function with another arg type", async() => {
        await db.query(`
            create function test(a numeric)
            returns integer as $$select 1$$
            language sql;
        `);

        await DdlManager.migrate(db, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {
                        language: "sql",
                        schema: "public",
                        name: "test",
                        args: [
                            {
                                name: "a",
                                type: "bigint"
                            }
                        ],
                        returns: {type: "integer"},
                        body: "select 2"
                    }
                ],
                triggers: []
            }
        });

        let result = await db.query("select test(1::bigint)");
        let row = result && result.rows[0];
        
        result = row.test;

        assert.equal(result, 2);

    });

    it("error on replace freeze trigger", async() => {
        await db.query(`
            create table company (
                id serial primary key
            );

            create function test()
            returns trigger as $$
            begin
                return new;
            end
            $$
            language plpgsql;
            
            create trigger x
            after insert
            on company
            for each row
            execute procedure test()
        `);

        let result = await DdlManager.migrate(db, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {
                        language: "plpgsql",
                        schema: "public",
                        name: "test2",
                        args: [],
                        returns: {type: "trigger"},
                        body: `
                            begin
                                return new;
                            end
                        `
                    }
                ],
                triggers: [
                    {
                        table: {
                            schema: "public",
                            name: "company"
                        },
                        name: "x",
                        after: true,
                        delete: true,
                        procedure: {
                            schema: "public",
                            name: "test"
                        }
                    }
                ]
            }
        });

        assert.equal(result.errors.length, 1);
        let err = result.errors[0];
        assert.equal(err.message, "cannot replace freeze trigger x on public.company");
    });

    it("error on drop freeze trigger", async() => {
        await db.query(`
            create table company (
                id serial primary key
            );

            create function test()
            returns trigger as $$
            begin
                return new;
            end
            $$
            language plpgsql;
            
            create trigger x
            after insert
            on company
            for each row
            execute procedure test()
        `);

        
        let result = await DdlManager.migrate(db, {
            drop: {
                functions: [
                    {
                        language: "plpgsql",
                        schema: "public",
                        name: "test2",
                        args: [],
                        returns: {type: "trigger"},
                        body: `
                            begin
                                return new;
                            end
                        `
                    }
                ],
                triggers: [
                    {
                        table: {
                            schema: "public",
                            name: "company"
                        },
                        name: "x",
                        after: true,
                        delete: true,
                        procedure: {
                            schema: "public",
                            name: "test"
                        }
                    }
                ]
            },
            create: {
                functions: [],
                triggers: []
            }
        });

        assert.equal(result.errors.length, 1);
        let err = result.errors[0];
        assert.equal(err.message, "cannot drop freeze trigger x on public.company");
    });


    it("migrate function with returns table", async() => {
        await db.query(`
            create table some_table (
                id serial primary key
            );
        `);

        await DdlManager.migrate(db, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {
                        language: "plpgsql",
                        schema: "public",
                        name: "test_func",
                        args: [],
                        returns: {
                            type: "public.some_table"
                        },
                        body: `
                        declare some_table_row some_table;
                        begin
                            select *
                            from some_table
                            into some_table_row;

                            return some_table_row;
                        end`
                    }
                ],
                triggers: []
            }
        });

        // expected execute without errors
        await db.query("select * from test_func()");
    });

    it("migrate function with returns setof table", async() => {
        await db.query(`
            create table some_table (
                id serial primary key
            );

            insert into some_table
            default values;

            insert into some_table
            default values;
        `);

        await DdlManager.migrate(db, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {
                        language: "plpgsql",
                        schema: "public",
                        name: "test_func",
                        args: [],
                        returns: {
                            setof: true,
                            type: "public.some_table"
                        },
                        body: `
                        begin
                            return query 
                                select *
                                from some_table;
                        end`
                    }
                ],
                triggers: []
            }
        });

        // expected execute without errors
        let result = await db.query("select * from test_func()");

        assert.deepEqual(result.rows, [
            {id: 1},
            {id: 2}
        ]);
    });

    it("migrate function with arg table", async() => {
        await db.query(`
            create table some_table (
                id serial primary key
            );
        `);

        await DdlManager.migrate(db, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {
                        language: "plpgsql",
                        schema: "public",
                        name: "test_func",
                        args: [
                            {
                                name: "some_table",
                                type: "public.some_table"
                            }
                        ],
                        returns: {type: "void"},
                        body: `
                        begin
                        end`
                    }
                ],
                triggers: []
            }
        });

        // expected execute without errors
        await db.query("select test_func(some_table) from some_table");
    });
});