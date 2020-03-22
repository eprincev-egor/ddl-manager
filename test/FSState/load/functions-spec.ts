import {TestState, ITestFiles} from "../TestState";

describe("FSState", () => {

    it("load dir with one file with function", async() => {
        const files: ITestFiles = {
            "test.sql": [
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
                    files: [
                        {
                            path: "test.sql",
                            name: "test.sql",
                            content: TestState.concatFilesSql( files["test.sql"] )
                        }
                    ],
                    folders: []
                },
                functions: [
                    {
                        filePath: "test.sql",
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
    
    it("load dir with one file with two functions", async() => {
        const files: ITestFiles = {
            "test.sql": [
                {
                    type: "function",
                    sql: `
                        create function test1()
                        returns integer as $body$
                        begin
                            return 1;
                        end
                        $body$
                        language plpgsql;
                    `,
                    row: {
                        identify: "test1()",
                        name: "test1"
                    }
                },
                {
                    type: "function",
                    sql: `
                        create function test2()
                        returns integer as $body$
                        begin
                            return 2;
                        end
                        $body$
                        language plpgsql;
                    `,
                    row: {
                        identify: "test2()",
                        name: "test2"
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
                    files: [
                        {
                            path: "test.sql",
                            name: "test.sql",
                            content: TestState.concatFilesSql( files["test.sql"] )
                        }
                    ],
                    folders: []
                },
                functions: [
                    {
                        filePath: "test.sql",
                        identify: "test1()",
                        name: "test1",
                        parsed: null,
                        createdByDDLManager: true
                    },
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
        });

    });
    
});