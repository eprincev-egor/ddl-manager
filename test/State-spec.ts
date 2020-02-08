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

            assert.deepStrictEqual(commands.toJSON(), [
                {
                    type: "create",
                    function: {
                        schema: "public",
                        name: "test",
                        args: ""
                    }
                }
            ]);
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

            assert.deepStrictEqual(commands.toJSON(), [
                {
                    type: "drop",
                    function: {
                        schema: "public",
                        name: "test",
                        args: ""
                    }
                }
            ]);
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

            assert.deepStrictEqual(commands.toJSON(), [
            ]);
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

            assert.deepStrictEqual(commands.toJSON(), [
                // first the 'drop'
                {
                    type: "drop",
                    function: {
                        schema: "public",
                        name: "test2",
                        args: ""
                    }
                },
                // second the 'create'
                {
                    type: "create",
                    function: {
                        schema: "public",
                        name: "test1",
                        args: ""
                    }
                }
            ]);
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

            assert.deepStrictEqual(commands.toJSON(), [
                {
                    type: "create",
                    view: {
                        schema: "public",
                        name: "operations_view"
                    }
                }
            ]);
        });

        
        it("remove view for db with one view", () => {
            
            const fsState = new State({
                views: []
            });

            const dbState = new State({
                views: [{
                    schema: "public",
                    name: "operations_view"
                }]
            });

            const migration = fsState.generateMigration(dbState);
            const commands = migration.get("commands");

            assert.deepStrictEqual(commands.toJSON(), [
                {
                    type: "drop",
                    view: {
                        schema: "public",
                        name: "operations_view"
                    }
                }
            ]);
        });

        it("create table for empty db", () => {
            
            const fsState = new State({
                tables: [{
                    schema: "public",
                    name: "company",
                    columns: [{
                        key: "id",
                        type: "integer"
                    }]
                }]
            });

            const dbState = new State({
                tables: []
            });

            const migration = fsState.generateMigration(dbState);
            const commands = migration.get("commands");

            assert.deepStrictEqual(commands.toJSON(), [
                {
                    type: "create",
                    table: {
                        schema: "public",
                        name: "company",
                        columns: [{
                            key: "id",
                            type: "integer"
                        }]
                    }
                }
            ]);
        });

        it("create column", () => {
            
            const fsState = new State({
                tables: [{
                    schema: "public",
                    name: "company",
                    columns: [{
                        key: "id",
                        type: "integer"
                    }, {
                        key: "name",
                        type: "text"
                    }]
                }]
            });

            const dbState = new State({
                tables: [{
                    schema: "public",
                    name: "company",
                    columns: [{
                        key: "id",
                        type: "integer"
                    }]
                }]
            });

            const migration = fsState.generateMigration(dbState);
            const commands = migration.get("commands");

            assert.deepStrictEqual(commands.toJSON(), [
                {
                    type: "create",
                    schema: "public",
                    table: "company",
                    column: {
                        key: "name",
                        type: "text"
                    }
                }
            ]);
        });

        
        it("db and fs has only one same table, empty migration", () => {
            
            const fsState = new State({
                tables: [{
                    schema: "public",
                    name: "company",
                    columns: [{
                        key: "id",
                        type: "integer"
                    }, {
                        key: "name",
                        type: "text"
                    }]
                }]
            });

            const dbState = new State({
                tables: [{
                    schema: "public",
                    name: "company",
                    columns: [{
                        key: "id",
                        type: "integer"
                    }, {
                        key: "name",
                        type: "text"
                    }]
                }]
            });

            const migration = fsState.generateMigration(dbState);
            const commands = migration.get("commands");

            assert.deepStrictEqual(commands.toJSON(), [
            ]);
        });
        
    });
    
});
