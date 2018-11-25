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
                        returns: "bigint",
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
                        returns: "trigger",
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
                        returns: "trigger",
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

        try {
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
                            args: [],
                            returns: "integer",
                            body: "select 2"
                        }
                    ],
                    triggers: []
                }
            });

            throw new Error("expected error");
        } catch(err) {
            assert.equal(err.message, "cannot replace freeze function public.test()");
        }
    });

    // it("freeze function with another args", async() => {
    //     await db.query(`
    //         create function test(a integer)
    //         returns integer as $$select 1$$
    //         language sql;
    //     `);

    //     await DdlManager.migrate(db, {
    //         drop: {
    //             functions: [],
    //             triggers: []
    //         },
    //         create: {
    //             functions: [
    //                 {
    //                     language: "sql",
    //                     schema: "public",
    //                     name: "test",
    //                     args: [
    //                         {
    //                             name: "a",
    //                             type: "integer"
    //                         },
    //                         {
    //                             name: "b",
    //                             type: "integer"
    //                         }
    //                     ],
    //                     returns: "integer",
    //                     body: "select 2"
    //                 }
    //             ],
    //             triggers: []
    //         }
    //     });

    //     let result = await db.query("select test(1, 2)");
    //     let row = result && result.rows[0];
        
    //     result = row.test;

    //     assert.equal(result, 2);

    // });

});