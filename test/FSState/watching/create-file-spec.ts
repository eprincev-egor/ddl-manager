import {FSTest} from "../FSTest";
import assert from "assert";

describe("FSDDLState, watching", () => {

    it("create empty state, then create file with function for test watching", async() => {
        const test = new FSTest({});
        const fsState = test.fsState;
        const fsController = test.controller;
        
        await fsController.load("./");
        
        assert.deepStrictEqual(
            fsState.toJSON(),
            {
                folder: {
                    name: "",
                    path: "./",
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


        test.setTestFile("test.sql", [
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
        await test.emitFS("change", "test.sql");

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
    });
    
});