import State from "../lib/State";
import assert from "assert";

describe("State", () => {

    describe("generateMigration", () => {
        
        it("create function for empty db", () => {
            
            const fsState = new State({
                functions: [{
                    schema: "public",
                    name: "test"
                }]
            });

            const dbState = new State({
                functions: []
            });

            const migration = fsState.generateMigration(dbState);
            const commands = migration.get("commands");

            assert.strictEqual(commands.length, 1);
            const firstCommand = commands.first();

            assert.strictEqual(firstCommand.get("type"), "CreateFunction");
        });

        it("remove function for db with one function", () => {
            
            const fsState = new State({
                functions: []
            });

            const dbState = new State({
                functions: [{
                    schema: "public",
                    name: "test"
                }]
            });

            const migration = fsState.generateMigration(dbState);
            const commands = migration.get("commands");

            assert.strictEqual(commands.length, 1);
            const firstCommand = commands.first();

            assert.strictEqual(firstCommand.get("type"), "DropFunction");
        });

        it("db and fs has only one function, empty migration", () => {
            
            const fsState = new State({
                functions: [{
                    schema: "public",
                    name: "test"
                }]
            });

            const dbState = new State({
                functions: [{
                    schema: "public",
                    name: "test"
                }]
            });

            const migration = fsState.generateMigration(dbState);
            const commands = migration.get("commands");

            assert.strictEqual(commands.length, 0);
        });

        it("db and fs has only one function, but that different functions", () => {
            
            const fsState = new State({
                functions: [{
                    schema: "public",
                    name: "test1"
                }]
            });

            const dbState = new State({
                functions: [{
                    schema: "public",
                    name: "test2"
                }]
            });

            const migration = fsState.generateMigration(dbState);
            const commands = migration.get("commands");

            assert.strictEqual(commands.length, 2);

            // first the 'drop'
            const firstCommand = commands.first();
            assert.strictEqual(firstCommand.get("type"), "DropFunction");
            assert.deepStrictEqual(firstCommand.get("function").toJSON(), {
                schema: "public",
                name: "test2",
                args: ""
            });
            
            // second the 'create'
            const lastCommand = commands.last();
            assert.strictEqual(lastCommand.get("type"), "CreateFunction");
            assert.deepStrictEqual(lastCommand.get("function").toJSON(), {
                schema: "public",
                name: "test1",
                args: ""
            });
        });

        it("create view for empty db", () => {
            
            const fsState = new State({
                views: [{
                    schema: "public",
                    name: "operations_view"
                }]
            });

            const dbState = new State({
                views: []
            });

            const migration = fsState.generateMigration(dbState);
            const commands = migration.get("commands");

            assert.strictEqual(commands.length, 1);
            const firstCommand = commands.first();

            assert.strictEqual(firstCommand.get("type"), "CreateView");
        });

        
        it("remove view for db with one view", () => {
            
            const fsState = new State({
                views: []
            });

            const dbState = new State({
                views: [{
                    schema: "public",
                    name: "test"
                }]
            });

            const migration = fsState.generateMigration(dbState);
            const commands = migration.get("commands");

            assert.strictEqual(commands.length, 1);
            const firstCommand = commands.first();

            assert.strictEqual(firstCommand.get("type"), "DropView");
        });

    });
    
});
