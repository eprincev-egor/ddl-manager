"use strict";

const assert = require("assert");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../lib/DdlManager");

describe("DlManager.migrateFunction, DlManager.migrateTrigger", () => {
    it("migrateFunction null", async() => {

        try {
            await DdlManager.migrateFunction(null, null);
            
            assert.ok(false, "expected error for null");
        } catch(err) {
            assert.equal(err.message, "invalid function");
        }
    });

    it("migrateTrigger null", async() => {

        try {
            await DdlManager.migrateTrigger(null, null);
            
            assert.ok(false, "expected error for null");
        } catch(err) {
            assert.equal(err.message, "invalid trigger");
        }
    });

    it("migrate simple function", async() => {
        let db = await getDbClient();
        
        await db.query(`
            drop function if exists test_migrate_function()
        `);

        let rnd = Math.round( 10000 * Math.random() );

        await DdlManager.migrateFunction(db, {
            language: "plpgsql",
            schema: "public",
            name: "test_migrate_function",
            args: [],
            returns: "bigint",
            body: `begin
                return ${ rnd };
            end`
        });

        let result = await db.query("select test_migrate_function()");
        let row = result && result.rows[0];
        
        result = row.test_migrate_function;

        assert.equal(result, rnd);

        // clear test state
        await db.query(`
            drop function if exists test_migrate_function()
        `);
        db.end();
    });

    it("migrate function and trigger", async() => {
        let db = await getDbClient();

        await db.query(`
            drop table if exists ddl_manager_test cascade;
            create table ddl_manager_test (
                name text,
                note text
            );
        `);

        await DdlManager.migrateFunction(db, {
            language: "plpgsql",
            schema: "public",
            name: "some_action_on_diu_test",
            args: [],
            returns: "trigger",
            body: `begin
            raise exception 'success';
            end`
        });

        await DdlManager.migrateTrigger(db, {
            table: {
                schema: "public",
                name: "ddl_manager_test"
            },
            after: true,
            insert: true,
            update: ["name", "note"],
            delete: true,
            procedure: {
                schema: "public",
                name: "some_action_on_diu_test"
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
        


        // clear test state
        await db.query(`
            drop table if exists ddl_manager_test cascade;
            drop function some_action_on_diu_test();
        `);
        db.end();
    });

    it("twice migrate function and trigger", async() => {
        let db = await getDbClient();

        await db.query(`
            drop table if exists ddl_manager_test cascade;
            create table ddl_manager_test (
                name text,
                note text
            );
        `);

        let file = {
            function: {
                language: "plpgsql",
                schema: "public",
                name: "some_action_on_diu_test",
                args: [],
                returns: "trigger",
                body: `begin
                    raise exception 'success';
                end`
            },
            trigger: {
                table: {
                    schema: "public",
                    name: "ddl_manager_test"
                },
                after: true,
                insert: true,
                update: ["name", "note"],
                delete: true,
                procedure: {
                    schema: "public",
                    name: "some_action_on_diu_test"
                }
            }
        };

        // do it twice without errors
        await DdlManager.migrateFunction(db, file.function);
        await DdlManager.migrateTrigger(db, file.trigger);


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
        


        // clear test state
        await db.query(`
            drop table if exists ddl_manager_test cascade;
            drop function some_action_on_diu_test();
        `);
        db.end();
    });
});