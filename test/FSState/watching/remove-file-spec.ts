import {FSTest} from "../FSTest";
import assert from "assert";

describe("FSDDLState, watching", () => {

    it("removing file", async() => {
        const test = new FSTest({
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
        const fsState = test.fsState;
        const fsController = test.controller;
        
        await fsController.load("./");

        // check first state
        assert.deepStrictEqual(
            fsState.toJSON(),
            {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        test.getFileJSON( "test.sql" )
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

        test.removeTestFile("test.sql");
        await test.emitFS("unlink", "test.sql");

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