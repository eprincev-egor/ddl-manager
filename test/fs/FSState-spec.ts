import FSState from "../../lib/FSState";
import TestFSDriver from "./TestFSDriver";
import TestParser from "./TestParser";
import assert from "assert";
import FunctionModel from "../../lib/objects/FunctionModel";

describe("FSState", () => {

    it("load dir with one file with function", async() => {
        const testFuncSQL = `
            create function public.test()
            returns void as $body$
            begin
            end
            $body$
            language plpgsql;
        `;

        const fsDriver = new TestFSDriver({
            files: {
                "test.sql": testFuncSQL
            },
            folders: {

            }
        });

        const testParser = new TestParser({
            [testFuncSQL]: [
                new FunctionModel({
                    schema: "public",
                    name: "test",
                    args: ""
                })
            ]
        });
        
        const fsState = new FSState({
            driver: fsDriver,
            parser: testParser
        });

        await fsState.load("./");

        assert.deepStrictEqual(fsState.toJSON(), {
            folder: {
                path: "./",
                name: "",
                files: [
                    {
                        path: "./test.sql",
                        name: "test.sql",
                        content: testFuncSQL
                    }
                ],
                folders: []
            },
            functions: [
                {
                    schema: "public",
                    name: "test",
                    args: ""
                }
            ],
            triggers: [],
            tables: [],
            views: []
        });
    });
    
});