import {TestState} from "../TestState";
import assert from "assert";

describe("FSState, watching", () => {

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
        await fsState.load("./");

        // check first state
        assert.deepStrictEqual(
            fsState.toJSON(),
            {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "test.sql",
                            name: "test.sql",
                            content: testState.getFileSQL( "test.sql" )
                        }
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
                views: []
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
                views: []
            }
        );

    });
    
});