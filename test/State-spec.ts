import State from "../lib/state/State";
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

        const migration = dbState.generateMigration(fsState);

        
    });

});
