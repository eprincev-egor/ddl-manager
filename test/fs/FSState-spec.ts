import FSState from "../../lib/FSState";
import TestFSDriver from "./TestFSDriver";
import assert from "assert";

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
        
        const fsState = new FSState({
            driver: fsDriver
        });

        await fsState.load("./");

        assert.deepStrictEqual(fsState.toJSON(), {
            driver: null,
            parser: null,
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