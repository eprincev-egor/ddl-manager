import assert from "assert";
import { getDBClient } from "./utils/getDbClient";
import { Migrator } from "../lib/Migrator";
import { IDiff } from "../lib/interface";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";

use(chaiShallowDeepEqualPlugin);

async function migrate(params: {db: any, diff: IDiff}) {
    const migrator = new Migrator(params.db);
    await migrator.migrate(params.diff);
}

describe("Migrator.migrate", () => {
    let db: any;
    
    beforeEach(async() => {
        db = await getDBClient();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);
    });

    afterEach(async() => {
        db.end();
    });

    it("migrate simple function", async() => {

        const rnd = Math.round( 10000 * Math.random() );
        
        await migrate({
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
                            body: {content: `begin
                            return ${ rnd };
                        end`}
                        }
                    ],
                    triggers: []
                }
            }
        });

        let result = await db.query("select test_migrate_function()");
        const row = result && result.rows[0];
        
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

        await migrate({
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
                            body: {content: `begin
                            raise exception 'success';
                        end`}
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
                            updateOf: ["name", "note"],
                            delete: true,
                            name: "some_action_on_diu_test_trigger",
                            procedure: {
                                schema: "public",
                                name: "some_action_on_diu_test",
                                args: []
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

        const diff: IDiff = {
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
                        body: {content: `begin
                            raise exception 'success';
                        end`}
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
                        updateOf: ["name", "note"],
                        delete: true,
                        name: "some_action_on_diu_test_trigger",
                        procedure: {
                            schema: "public",
                            name: "some_action_on_diu_test",
                            args: []
                        }
                    }
                ]
            }
        };

        // do it twice without errors
        await migrate({db, diff});
        await migrate({db, diff});
        

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

    it("no error on replace frozen function", async() => {
        await db.query(`
            create function test()
            returns integer as $$select 1$$
            language sql;
        `);

        await migrate({db, diff: {
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
                        body: {content: "select 2"}
                    }
                ],
                triggers: []
            }
        }});
        
        let result = await db.query("select test()");
        const row = result && result.rows[0];
        
        result = row.test;

        assert.equal(result, 2);
    });

    it("error on drop frozen function", async() => {
        await db.query(`
            create function test()
            returns integer as $$select 1$$
            language sql;
        `);

        try {
            await migrate({db, diff: {
                drop: {
                    functions: [
                        {
                            language: "sql",
                            schema: "public",
                            name: "test",
                            args: [],
                            returns: {type: "integer"},
                            body: {content: "select 2"}
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
            assert.equal(err.message, "public.test()\ncannot drop frozen function public.test()");
        }
    });

    it("frozen function with another args", async() => {
        await db.query(`
            create function test(a integer)
            returns integer as $$select 1$$
            language sql;
        `);

        await migrate({db, diff: {
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
                        body: {content: "select 2"}
                    }
                ],
                triggers: []
            }
        }});

        let result = await db.query("select test(1, 2)");
        const row = result && result.rows[0];
        
        result = row.test;

        assert.equal(result, 2);

    });

    
    it("frozen function with another arg type", async() => {
        await db.query(`
            create function test(a numeric)
            returns integer as $$select 1$$
            language sql;
        `);

        await migrate({db, diff: {
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
                        body: {content: "select 2"}
                    }
                ],
                triggers: []
            }
        }});

        let result = await db.query("select test(1::bigint)");
        const row = result && result.rows[0];
        
        result = row.test;

        assert.equal(result, 2);

    });

    it("error on replace frozen trigger", async() => {
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
            await migrate({db, diff: {
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
                            body: {content: `
                                begin
                                    return new;
                                end
                            `}
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
                                name: "test",
                                args: []
                            }
                        }
                    ]
                }
            }});
        } catch(err) {
            assert.equal(err.message, "x on public.company\ncannot replace frozen trigger x on public.company");
        }
    });

    it("error on drop frozen trigger", async() => {
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
            await migrate({db, diff: {
                drop: {
                    functions: [
                        {
                            language: "plpgsql",
                            schema: "public",
                            name: "test2",
                            args: [],
                            returns: {type: "trigger"},
                            body: {content: `
                            begin
                                return new;
                            end
                        `}
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
                                name: "test",
                                args: []
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
            assert.equal(err.message, "x on public.company\ncannot drop frozen trigger x on public.company");
        }
    });


    it("migrate function with returns table", async() => {
        await db.query(`
            create table some_table (
                id serial primary key
            );
        `);

        await migrate({db, diff: {
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
                        body: {content: `
                        declare some_table_row some_table;
                        begin
                            select *
                            from some_table
                            into some_table_row;

                            return some_table_row;
                        end`}
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

        await migrate({db, diff: {
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
                        body: {content: `
                        begin
                            return query 
                                select *
                                from some_table;
                        end`}
                    }
                ],
                triggers: []
            }
        }});

        // expected execute without errors
        const result = await db.query("select * from test_func()");

        expect(result.rows).to.be.shallowDeepEqual([
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

        await migrate({db, diff: {
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
                        body: {content: `
                        begin
                        end`}
                    }
                ],
                triggers: []
            }
        }});

        // expected execute without errors
        await db.query("select test_func(some_table) from some_table");
    });

    it("migrate function, arg without name", async() => {
        await migrate({db, diff: {
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
                                name: null as any,
                                type: "text"
                            }
                        ],
                        returns: {type: "void"},
                        body: {content: `
                        begin
                        end`}
                    }
                ],
                triggers: []
            }
        }});

        // expected execute without errors
        await db.query("select test_func('')");
    });

    it("migrate function, in/out arg", async() => {
        await migrate({db, diff: {
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
                        body: {content: `
                        begin
                            name = 'nice' || id::text;
                        end`}
                    }
                ],
                triggers: []
            }
        }});

        const result = await db.query("select test_func(1) as test");

        expect(result.rows[0]).to.be.shallowDeepEqual({
            test: "nice1"
        });
    });

    it("migrate simple function with comment", async() => {

        await migrate({
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
                            body: {content: `begin
                                return 1;
                            end`},
                            comment: "nice"
                        }
                    ],
                    triggers: []
                }
            }
        });

        const result = await db.query(`
            select
                pg_catalog.obj_description( pg_proc.oid ) as comment
            from information_schema.routines as routines

            left join pg_catalog.pg_proc as pg_proc on
                routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text
        
            where
                routines.routine_schema = 'public' and
                routines.routine_name = 'some_func'
        `);

        expect(result.rows[0]).to.be.shallowDeepEqual({
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

        await migrate({
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
                            body: {content: `begin
                            raise exception 'success';
                        end`}
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
                            updateOf: ["name", "note"],
                            delete: true,
                            name: "some_action_on_diu_test_trigger",
                            procedure: {
                                schema: "public",
                                name: "some_action_on_diu_test",
                                args: []
                            },
                            comment: "super"
                        }
                    ]
                }
            }
        });

        const result = await db.query(`
            select
                pg_catalog.obj_description( pg_trigger.oid ) as comment
            from pg_trigger
            where
                pg_trigger.tgname = 'some_action_on_diu_test_trigger'
        `);

        expect(result.rows[0]).to.be.shallowDeepEqual({
            comment: "super\nddl-manager-sync"
        });

    });

    it("migrate function, arg default null", async() => {
        const func = {
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
            body: {content: `
            begin
                return 'nice' || coalesce(id, 2)::text;
            end`}
        };

        await migrate({db, diff: {
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

        const result = await db.query("select test_func() as test");

        expect(result.rows[0]).to.be.shallowDeepEqual({
            test: "nice2"
        });


        await migrate({db, diff: {
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

    it("migrate two functions, same name, diff args", async() => {
        const func1 = {
            language: "plpgsql",
            schema: "public",
            name: "test_func",
            args: [
                {
                    name: "x",
                    type: "integer",
                    default: "null"
                }
            ],
            returns: {type: "integer"},
            body: {content: `
            begin
                return 1;
            end`}
        };
        const func2 = {
            language: "plpgsql",
            schema: "public",
            name: "test_func",
            args: [
                {
                    name: "x",
                    type: "boolean",
                    default: "null"
                }
            ],
            returns: {type: "integer"},
            body: {content: `
            begin
                return 2;
            end`}
        };

        await migrate({db, diff: {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    func1,
                    func2
                ],
                triggers: []
            }
        }});

        let result;

        result = await db.query("select test_func(1) as test1");
        expect(result.rows[0]).to.be.shallowDeepEqual({
            test1: 1
        });

        result = await db.query("select test_func(true) as test2");
        expect(result.rows[0]).to.be.shallowDeepEqual({
            test2: 2
        });


        await migrate({db, diff: {
            drop: {
                functions: [
                    func1,
                    func2
                ],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        }});

        // old functions must be dropped
        try {
            await db.query("select test_func(1) as nice");
            assert.ok(false, "expected error");
        } catch(err) {
            assert.equal(err.message, "function test_func(integer) does not exist");
        }

        try {
            await db.query("select test_func(true) as nice");
            assert.ok(false, "expected error");
        } catch(err) {
            assert.equal(err.message, "function test_func(boolean) does not exist");
        }

    });

});