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
                                filePath: "./company.sql",
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
                        filePath: "./company.sql",
                        identify: "public.company",
                        parsed: null,
                        name: "company",
                        columns: [
                            {
                                filePath: "./company.sql",
                                identify: "id",
                                key: "id",
                                type: "integer",
                                nulls: true,
                                parsed: null
                            }
                        ],
                        deprecated: false,
                        deprecatedColumns: [],
                        primaryKey: ["id"],
                        checkConstraints: [],
                        foreignKeysConstraints: [],
                        uniqueConstraints: [],
                        rows: null
                    }
                ],
                triggers: [],
                functions: [],
                views: []
            }
        });

    });
    
});