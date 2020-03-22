import {TestState, ITestFiles} from "../TestState";

describe("FSState", () => {

    it("load sub dir", async() => {
        const files: ITestFiles = {
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
        };

        await TestState.testLoading({
            files,
            expectedState: {
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
                                        {
                                            path: "sub/dir/test.sql",
                                            name: "test.sql",
                                            content: TestState.concatFilesSql( files["./sub/dir/test.sql"] )
                                        }
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
            }
        });

    });
    
});