import testLoadState, {ITestFiles} from "./testLoadState";

describe("FSState", () => {

    it("load dir with one file with table", async() => {
        const files: ITestFiles = {
            "./company.sql": [
                {
                    type: "table",
                    sql: `
                        create table company (
                            id serial primary key
                        )
                    `,
                    row: {
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }
                        ],
                        primaryKey: ["id"]
                    }
                }
            ]
        };

        await testLoadState({
            files,
            expectedState: {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "./company.sql",
                            name: "company.sql",
                            content: testLoadState.getFileSql(files["./company.sql"])
                        }
                    ],
                    folders: []
                },
                tables: [
                    {
                        filePath: "./test.sql",
                        identify: "public.test()",
                        name: "test",
                        parsed: null
                    }
                ],
                triggers: [],
                functions: [],
                views: []
            }
        });

    });
    
});