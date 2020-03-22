import {TestState} from "../TestState";
import assert from "assert";

describe("FSState, watching", () => {

    it("create empty state, then create file with function for test watching", async() => {
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

        testState.setTestFile("test.sql", [
            {
                type: "function",
                sql: `
                    create or replace function test2()
                    returns void as $body$
                    begin

                    end
                    $body$
                    language plpgsql;
                `,
                row: {
                    identify: "test2()",
                    name: "test2"
                }
            }
        ]);

        await testState.emitFS("change", "test.sql");

        // check changed state
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
                        identify: "test2()",
                        name: "test2",
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