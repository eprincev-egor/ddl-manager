import {TestState} from "../TestState";
import assert from "assert";

describe("FSState, watching", () => {

    it("create empty state, then create file with function for test watching", async() => {
        const testState = new TestState({});
        const fsState = testState.fsState;

        assert.deepStrictEqual(
            fsState.toJSON(),
            {
                folder: null,
                functions: [],
                triggers: [],
                tables: [],
                views: []
            }
        );

        testState.setTestFile("./test.sql", [
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
        ]);
        await testState.emitFS("change", "./test.sql");

        assert.deepStrictEqual(
            fsState.toJSON(),
            {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "./test.sql",
                            name: "test.sql",
                            content: testState.getFileSQL( "./test.sql" )
                        }
                    ],
                    folders: []
                },
                functions: [
                    {
                        filePath: "./test.sql",
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
    });
    
});