import testLoadState, {ITestFiles} from "./testLoadState";

describe("FSState", () => {

    it("load dir with one file with function", async() => {
        const files: ITestFiles = {
            "test.sql": {
                sql: `
                    create function public.test()
                    returns void as $body$
                    begin
                    end
                    $body$
                    language plpgsql;
                `,
                models: [
                    {
                        type: "function",
                        row: {
                            identify: "public.test()",
                            name: "test"
                        }
                    }
                ]
            }
        };

        testLoadState({
            files,
            folders: {},
            expectedState: {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "./test.sql",
                            name: "test.sql",
                            content: files["test.sql"].sql
                        }
                    ],
                    folders: []
                },
                functions: [
                    {
                        filePath: "./test.sql",
                        identify: "public.test()",
                        name: "test",
                        parsed: null
                    }
                ],
                triggers: [],
                tables: [],
                views: []
            }
        });

    });
    
});