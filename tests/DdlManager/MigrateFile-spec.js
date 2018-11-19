"use strict";

const assert = require("assert");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../DdlManager");

describe("DlManager.migrateFile", () => {
    it("migrate null", async() => {

        try {
            await DdlManager.migrateFile(null, null);
            
            assert.ok(false, "expected error for null");
        } catch(err) {
            assert.equal(err.message, "invalid function");
        }
    });

    it("migrate simple function", async() => {
        let db = await getDbClient();
        
        await db.query(`
            drop function if exists test_migrate_function()
        `);

        let rnd = Math.round( 10000 * Math.random() );

        await DdlManager.migrateFile(db, {
            function: {
                schema: "public",
                name: "test_migrate_function",
                args: [],
                returns: "bigint",
                body: `begin
                    return ${ rnd };
                end`
            }
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

        await DdlManager.migrateFile(db, {
            function: {
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
                delete: true
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
                delete: true
            }
        };

        // do it twice without errors
        await DdlManager.migrateFile(db, file);
        await DdlManager.migrateFile(db, file);


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