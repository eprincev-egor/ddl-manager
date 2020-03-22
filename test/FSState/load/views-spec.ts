import {TestState, ITestFiles} from "../TestState";

describe("FSDDLState", () => {

    it("load dir with one file with view", async() => {
        const files: ITestFiles = {
            "view.sql": [
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

        await TestState.testLoading({
            files,
            expectedState: {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "view.sql",
                            name: "view.sql",
                            content: TestState.concatFilesSql( files["view.sql"] )
                        }
                    ],
                    folders: []
                },
                views: [
                    {
                        filePath: "view.sql",
                        identify: "public.companies",
                        name: "companies",
                        parsed: null,
                        createdByDDLManager: true
                    }
                ],
                triggers: [],
                tables: [],
                functions: [],
                extensions: []
            }
        });

    });
    
    it("load dir with one file with two views", async() => {
        const files: ITestFiles = {
            "view.sql": [
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

        await TestState.testLoading({
            files,
            expectedState: {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "view.sql",
                            name: "view.sql",
                            content: TestState.concatFilesSql( files["view.sql"] )
                        }
                    ],
                    folders: []
                },
                views: [
                    {
                        filePath: "view.sql",
                        identify: "public.companies",
                        name: "companies",
                        parsed: null,
                        createdByDDLManager: true
                    },
                    {
                        filePath: "view.sql",
                        identify: "public.orders",
                        name: "orders",
                        parsed: null,
                        createdByDDLManager: true
                    }
                ],
                triggers: [],
                tables: [],
                functions: [],
                extensions: []
            }
        });

    });
    
});