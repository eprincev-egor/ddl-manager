import {FSTest} from "../FSTest";

describe("FSDDLState", () => {

    it("read only sql files", async() => {
        const test = new FSTest({
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
            views: [],
            extensions: []
        });

    });
    
});