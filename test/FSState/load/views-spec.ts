import {TestState} from "../TestState";

describe("FSDDLState", () => {

    it("load dir with one file with view", async() => {
        
        const test = new TestState({
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
        });

        await test.testLoading({
            folder: {
                path: "./",
                name: "",
                files: [
                    test.getFileJSON("view.sql")
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
        });
    });
    
    it("load dir with one file with two views", async() => {
        const test = new TestState({
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
        });

        await test.testLoading({
            folder: {
                path: "./",
                name: "",
                files: [
                    test.getFileJSON("view.sql")
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
        });

    });
    
});