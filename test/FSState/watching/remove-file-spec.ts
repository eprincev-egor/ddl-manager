import {TestState} from "../TestState";
import assert from "assert";

describe("FSDDLState, watching", () => {

    it("removing file", async() => {
        const testState = new TestState({
            "test.sql": [
                {
                    type: "function",
                    sql: `
                        create or replace function test()
                        returns void as $body$
                        begin
    
                        end
                        $body$
                        language plpgsql;
                    `,
                    row: {
                        identify: "test()",
                        name: "test"
                    }
                }
            ]
        });
        const fsState = testState.fsState;
        const fsController = testState.controller;
        
        await fsController.load("./");

        // check first state
        assert.deepStrictEqual(
            fsState.toJSON(),
            {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        testState.getFileJSON( "test.sql" )
                    ],
                    folders: []
                },
                functions: [
                    {
                        filePath: "test.sql",
                        identify: "test()",
                        name: "test",
                        parsed: null,
                        createdByDDLManager: true
                    }
                ],
                triggers: [],
                tables: [],
                views: [],
                extensions: []
            }
        );

        testState.removeTestFile("test.sql");
        await testState.emitFS("unlink", "test.sql");

        // check changed state
        assert.deepStrictEqual(
            fsState.toJSON(),
            {
                folder: {
                    path: "./",
                    name: "",
                    files: [],
                    folders: []
                },
                functions: [],
                triggers: [],
                tables: [],
                views: [],
                extensions: []
            }
        );

    });
    
});