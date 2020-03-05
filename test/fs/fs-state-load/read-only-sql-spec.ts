import {TestState, ITestFiles} from "../TestState";

describe("FSState", () => {

    it("read only sql files", async() => {
        const files: ITestFiles = {
            "./sub/dir/test.md": [
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
                            path: "./sub",
                            name: "sub",
                            files: [],
                            folders: [
                                {
                                    path: "./sub/dir",
                                    name: "dir",
                                    files: [],
                                    folders: []
                                }
                            ]
                        }
                    ]
                },
                functions: [],
                triggers: [],
                tables: [],
                views: []
            }
        });

    });
    
});