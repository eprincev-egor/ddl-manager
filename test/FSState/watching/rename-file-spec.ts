import {TestState} from "../TestState";
import assert from "assert";

describe("FSState, watching", () => {

    it("rename file with one function", async() => {
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
                views: [],
                extensions: []
            }
        );

        testState.moveTestFile("test.sql", "test2.sql");

        await testState.emitFS("change", "test2.sql");

        // check changed state
        assert.deepStrictEqual(
            fsState.toJSON(),
            {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "test2.sql",
                            name: "test2.sql",
                            content: testState.getFileSQL( "test2.sql" )
                        }
                    ],
                    folders: []
                },
                functions: [
                    {
                        filePath: "test2.sql",
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

    });
    
});