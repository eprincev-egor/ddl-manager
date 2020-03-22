import {TestState} from "../TestState";

describe("FSDDLState", () => {

    it("load dir with one file with table", async() => {
        const test = new TestState({
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
        });

        await test.testLoading({
            folder: {
                path: "./",
                name: "",
                files: [
                    test.getFileJSON("company.sql")
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
            views: [],
            extensions: []
        });

    });
    
    
    it("load dir with one file with two tables", async() => {
        const test = new TestState({
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
        });

        await test.testLoading({
            folder: {
                path: "./",
                name: "",
                files: [
                    test.getFileJSON("tables.sql")
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
            views: [],
            extensions: []
        });

    });
    
    
});