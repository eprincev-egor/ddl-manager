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
            await DdlManager.migrate({
                db: null, 
                diff: null
            });
            
            assert.ok(false, "expected error for null");
        } catch(err) {
            assert.equal(err.message, "invalid diff");
        }
    });

    it("migrate simple function", async() => {

        let rnd = Math.round( 10000 * Math.random() );
        
        await DdlManager.migrate({
            db, 
            diff: {
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

        await DdlManager.migrate({
            db, 
            diff: {
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
        await DdlManager.migrate({db, diff});
        await DdlManager.migrate({db, diff});
        

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

        try {
            await DdlManager.migrate({db, diff: {
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
            }});
        } catch(err) {
            assert.equal(err.message, "Error: cannot replace freeze function public.test()");
        }
        
    });

    it("error on drop freeze function", async() => {
        await db.query(`
            create function test()
            returns integer as $$select 1$$
            language sql;
        `);

        try {
            await DdlManager.migrate({db, diff: {
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
            }});
        } catch(err) {
            assert.equal(err.message, "Error: cannot drop freeze function public.test()");
        }
    });

    it("freeze function with another args", async() => {
        await db.query(`
            create function test(a integer)
            returns integer as $$select 1$$
            language sql;
        `);

        await DdlManager.migrate({db, diff: {
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
        }});

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

        await DdlManager.migrate({db, diff: {
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
        }});

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

        try {
            await DdlManager.migrate({db, diff: {
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
            }});
        } catch(err) {
            assert.equal(err.message, "Error: cannot replace freeze trigger x on public.company");
        }
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

        
        try {
            await DdlManager.migrate({db, diff: {
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
            }});
        } catch(err) {
            assert.equal(err.message, "Error: cannot drop freeze trigger x on public.company");
        }
    });


    it("migrate function with returns table", async() => {
        await db.query(`
            create table some_table (
                id serial primary key
            );
        `);

        await DdlManager.migrate({db, diff: {
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
        }});

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

        await DdlManager.migrate({db, diff: {
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
        }});

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

        await DdlManager.migrate({db, diff: {
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
        }});

        // expected execute without errors
        await db.query("select test_func(some_table) from some_table");
    });

    it("migrate function, arg without name", async() => {
        await DdlManager.migrate({db, diff: {
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
                                name: false,
                                type: "text"
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
        }});

        // expected execute without errors
        await db.query("select test_func('')");
    });

    it("migrate function, in/out arg", async() => {
        await DdlManager.migrate({db, diff: {
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
                                in: true,
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
                        body: `
                        begin
                            name = 'nice' || id::text;
                        end`
                    }
                ],
                triggers: []
            }
        }});

        let result = await db.query("select test_func(1) as test");

        assert.deepEqual(result.rows[0], {
            test: "nice1"
        });
    });

    it("migrate simple function with comment", async() => {

        await DdlManager.migrate({
            db, 
            diff: {
                drop: {
                    functions: [],
                    triggers: []
                },
                create: {
                    functions: [
                        {
                            language: "plpgsql",
                            schema: "public",
                            name: "some_func",
                            args: [],
                            returns: {type: "bigint"},
                            body: `begin
                                return 1;
                            end`
                        }
                    ],
                    comments: [
                        {
                            function: {
                                schema: "public",
                                name: "some_func",
                                args: []
                            },
                            comment: "nice"
                        }
                    ],
                    triggers: []
                }
            }
        });

        let result = await db.query(`
            select
                pg_catalog.obj_description( pg_proc.oid ) as comment
            from information_schema.routines as routines

            left join pg_catalog.pg_proc as pg_proc on
                routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text
        
            where
                routines.routine_schema = 'public' and
                routines.routine_name = 'some_func'
        `);

        assert.deepEqual(result.rows[0], {
            comment: "nice\nddl-manager-sync"
        });

    });

    it("migrate simple trigger with comment", async() => {

        await db.query(`
            create table ddl_manager_test (
                name text,
                note text
            );
        `);

        await DdlManager.migrate({
            db, 
            diff: {
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
                    ],
                    comments: [
                        {
                            trigger: {
                                schema: "public",
                                table: "ddl_manager_test",
                                name: "some_action_on_diu_test_trigger"
                            },
                            comment: "super"
                        }
                    ]
                }
            }
        });

        let result = await db.query(`
            select
                pg_catalog.obj_description( pg_trigger.oid ) as comment
            from pg_trigger
            where
                pg_trigger.tgname = 'some_action_on_diu_test_trigger'
        `);

        assert.deepEqual(result.rows[0], {
            comment: "super\nddl-manager-sync"
        });

    });


    it("migrate drop function comment", async() => {

        await db.query(`
            create function test(a numeric)
            returns integer as $$select 1$$
            language sql;

            comment on function public.test(numeric) is $$xx$$
        `);

        await DdlManager.migrate({
            db, 
            diff: {
                drop: {
                    functions: [],
                    triggers: [],
                    comments: [
                        {
                            function: {
                                schema: "public",
                                name: "test",
                                args: ["numeric"]
                            },
                            comment: "xx"
                        }
                    ]
                },
                create: {
                    functions: [
                    ],
                    triggers: []
                }
            }
        });

        let result = await db.query(`
            select
                coalesce(
                    pg_catalog.obj_description( pg_proc.oid ),
                    'dropped'
                ) as comment
            from information_schema.routines as routines

            left join pg_catalog.pg_proc as pg_proc on
                routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text
        
            where
                routines.routine_schema = 'public' and
                routines.routine_name = 'test'
        `);

        assert.deepEqual(result.rows[0], {
            comment: "dropped"
        });

    });

    it("migrate drop trigger comment", async() => {

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
            execute procedure test();

            comment on trigger x on company is $$xx$$
        `);

        await DdlManager.migrate({
            db, 
            diff: {
                drop: {
                    functions: [],
                    triggers: [],
                    comments: [
                        {
                            trigger: {
                                schema: "public",
                                table: "company",
                                name: "x"
                            },
                            comment: "xx"
                        }
                    ]
                },
                create: {
                    functions: [
                    ],
                    triggers: []
                }
            }
        });

        let result = await db.query(`
            select
                coalesce(
                    pg_catalog.obj_description( pg_trigger.oid ),
                    'dropped'
                ) as comment
            from pg_trigger
            where
                pg_trigger.tgname = 'x'
        `);

        assert.deepEqual(result.rows[0], {
            comment: "dropped"
        });

    });


    it("migrate function, arg default null", async() => {
        let func = {
            language: "plpgsql",
            schema: "public",
            name: "test_func",
            args: [
                {
                    name: "id",
                    type: "integer",
                    default: "null"
                }
            ],
            returns: {type: "text"},
            body: `
            begin
                return 'nice' || coalesce(id, 2)::text;
            end`
        };

        await DdlManager.migrate({db, diff: {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    func
                ],
                triggers: []
            }
        }});

        let result = await db.query("select test_func() as test");

        assert.deepEqual(result.rows[0], {
            test: "nice2"
        });


        await DdlManager.migrate({db, diff: {
            drop: {
                functions: [
                    func
                ],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        }});

        // old function must be dropped
        try {
            await db.query("select test_func(1) as nice");
            assert.ok(false, "expected error");
        } catch(err) {
            assert.equal(err.message, "function test_func(integer) does not exist");
        }

        try {
            await db.query("select test_func() as nice");
            assert.ok(false, "expected error");
        } catch(err) {
            assert.equal(err.message, "function test_func() does not exist");
        }
    });

});