import testLoadState, {ITestFiles} from "./testLoadState";

describe("FSState", () => {

    it("load dir with one file with view", async() => {
        const files: ITestFiles = {
            "./view.sql": [
                {
                    type: "view",
                    sql: `
                        create view companies as
                            select 1
                    `,
                    row: {
                        identify: "public.companies",
                        name: "companies"
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
                            path: "./view.sql",
                            name: "view.sql",
                            content: testLoadState.getFileSql( files["./view.sql"] )
                        }
                    ],
                    folders: []
                },
                views: [
                    {
                        filePath: "./view.sql",
                        identify: "public.companies",
                        name: "companies",
                        parsed: null,
                        createdByDDLManager: true
                    }
                ],
                triggers: [],
                tables: [],
                functions: []
            }
        });

    });
    
    it("load dir with one file with two views", async() => {
        const files: ITestFiles = {
            "./view.sql": [
                {
                    type: "view",
                    sql: `
                        create view companies as
                            select 1
                    `,
                    row: {
                        identify: "public.companies",
                        name: "companies"
                    }
                },
                {
                    type: "view",
                    sql: `
                        create view orders as
                            select 1
                    `,
                    row: {
                        identify: "public.orders",
                        name: "orders"
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
                            path: "./view.sql",
                            name: "view.sql",
                            content: testLoadState.getFileSql( files["./view.sql"] )
                        }
                    ],
                    folders: []
                },
                views: [
                    {
                        filePath: "./view.sql",
                        identify: "public.companies",
                        name: "companies",
                        parsed: null,
                        createdByDDLManager: true
                    },
                    {
                        filePath: "./view.sql",
                        identify: "public.orders",
                        name: "orders",
                        parsed: null,
                        createdByDDLManager: true
                    }
                ],
                triggers: [],
                tables: [],
                functions: []
            }
        });

    });
    
});