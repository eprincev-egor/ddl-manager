import {TestState, ITestFiles} from "../TestState";

describe("FSState", () => {

    it("load dir with one file with table", async() => {
        const files: ITestFiles = {
            "company.sql": [
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
                                filePath: "company.sql",
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

        await TestState.testLoading({
            files,
            expectedState: {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "company.sql",
                            name: "company.sql",
                            content: TestState.concatFilesSql(files["company.sql"])
                        }
                    ],
                    folders: []
                },
                tables: [
                    {
                        filePath: "company.sql",
                        identify: "public.company",
                        parsed: null,
                        name: "company",
                        columns: [
                            {
                                filePath: "company.sql",
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
                        values: null
                    }
                ],
                triggers: [],
                functions: [],
                views: []
            }
        });

    });
    
    
    it("load dir with one file with two tables", async() => {
        const files: ITestFiles = {
            "tables.sql": [
                {
                    type: "table",
                    sql: `
                        create table company (
                            id serial primary key
                        );
                    `,
                    row: {
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                filePath: "tables.sql",
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }
                        ],
                        primaryKey: ["id"]
                    }
                },
                {
                    type: "table",
                    sql: `
                        create table orders (
                            id serial primary key,
                            id_company integer not null
                        );
                    `,
                    row: {
                        identify: "public.orders",
                        name: "orders",
                        columns: [
                            {
                                filePath: "tables.sql",
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                filePath: "tables.sql",
                                identify: "id_company",
                                key: "id_company",
                                type: "integer",
                                nulls: false
                            }
                        ],
                        primaryKey: ["id"]
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
                            path: "tables.sql",
                            name: "tables.sql",
                            content: TestState.concatFilesSql(files["tables.sql"])
                        }
                    ],
                    folders: []
                },
                tables: [
                    {
                        filePath: "tables.sql",
                        identify: "public.company",
                        parsed: null,
                        name: "company",
                        columns: [
                            {
                                filePath: "tables.sql",
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
                        values: null
                    },
                    {
                        filePath: "tables.sql",
                        identify: "public.orders",
                        parsed: null,
                        name: "orders",
                        columns: [
                            {
                                filePath: "tables.sql",
                                identify: "id",
                                key: "id",
                                type: "integer",
                                nulls: true,
                                parsed: null
                            },
                            {
                                filePath: "tables.sql",
                                identify: "id_company",
                                key: "id_company",
                                type: "integer",
                                nulls: false,
                                parsed: null
                            }
                        ],
                        deprecated: false,
                        deprecatedColumns: [],
                        primaryKey: ["id"],
                        checkConstraints: [],
                        foreignKeysConstraints: [],
                        uniqueConstraints: [],
                        values: null
                    }
                ],
                triggers: [],
                functions: [],
                views: []
            }
        });

    });
    
    
});