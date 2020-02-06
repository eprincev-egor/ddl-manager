import State from "../lib/State";
import assert from "assert";

describe("State", () => {
    
    it("migrate simple function", () => {
        
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

});
