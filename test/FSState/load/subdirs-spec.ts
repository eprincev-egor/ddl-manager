import {TestState} from "../TestState";

describe("FSDDLState", () => {

    it("load sub dir", async() => {
        const test = new TestState({
            "./sub/dir/test.sql": [
                {
                    type: "function",
                    sql: `
                        create function public.test()
                        returns void as $body$
                        begin
                        end
                        $body$
                        language plpgsql;
                    `,
                    row: {
                        identify: "public.test()",
                        name: "test"
                    }
                }
            ]
        });

        await test.testLoading({
            folder: {
                path: "./",
                name: "",
                files: [],
                folders: [
                    {
                        path: "sub",
                        name: "sub",
                        files: [],
                        folders: [
                            {
                                path: "sub/dir",
                                name: "dir",
                                files: [
                                    test.getFileJSON( "sub/dir/test.sql" )
                                ],
                                folders: []
                            }
                        ]
                    }
                ]
            },
            functions: [
                {
                    filePath: "./sub/dir/test.sql",
                    identify: "public.test()",
                    name: "test",
                    parsed: null,
                    createdByDDLManager: true
                }
            ],
            triggers: [],
            tables: [],
            views: [],
            extensions: []
        });

    });
    
});