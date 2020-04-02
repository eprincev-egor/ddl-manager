import {testGenerateMigration} from "../testGenerateMigration";
import { table, columnID, columnNAME } from "../fixtures/tables";

describe("Migration: tables", () => {

    describe("values (rows)", () => {
        
        const orderType = table("order_type", columnID, columnNAME);
        
        it("create rows, expected primary key error", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...orderType,
                        values: [
                            ["1", "FCL"]
                        ]
                    }]
                },
                db: {
                    tables: [
                        orderType
                    ]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "order_type.sql",
                            code: "ExpectedPrimaryKeyForRowsError",
                            message: "table public.order_type should have primary key for creating rows",
                            tableIdentify: "public.order_type"
                        }                        
                    ]
                }
            });
        });
        
        it("create rows, expected primary key error, empty db", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...orderType,
                        values: [
                            ["1", "FCL"]
                        ]
                    }]
                },
                db: {
                    tables: []
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "order_type.sql",
                            code: "ExpectedPrimaryKeyForRowsError",
                            message: "table public.order_type should have primary key for creating rows",
                            tableIdentify: "public.order_type"
                        }                        
                    ]
                }
            });
        });
        
        it("create rows, db table exists", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...orderType,
                        primaryKey: ["id"],
                        values: [
                            ["1", "FCL"]
                        ]
                    }]
                },
                db: {
                    tables: [
                        {
                            ...orderType,
                            primaryKey: ["id"]
                        }
                    ]
                },
                migration: {
                    commands: [
                        
                        {
                            type: "create",
                            command: "Rows",
                            table: {
                                filePath: "order_type.sql",
                                identify: "public.order_type",
                                name: "order_type",
                                parsed: null,
                                deprecatedColumns: [],
                                deprecated: false,
                                uniqueConstraints: [],
                                foreignKeysConstraints: [],
                                checkConstraints: [],
                                columns: [
                                    {
                                        filePath: null,
                                        identify: "id",
                                        key: "id",
                                        parsed: null,
                                        nulls: true,
                                        type: "integer"
                                    },
                                    {
                                        filePath: null,
                                        identify: "name",
                                        key: "name",
                                        parsed: null,
                                        nulls: true,
                                        type: "text"
                                    }
                                ],
                                primaryKey: ["id"],
                                values: [
                                    ["1", "FCL"]
                                ]
                            },
                            values: [
                                ["1", "FCL"]
                            ]
                        }
                    ],
                    errors: []
                }
            });
        });

        it("create table and rows, db is empty", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...orderType,
                        primaryKey: ["id"],
                        values: [
                            ["1", "FCL"]
                        ]
                    }]
                },
                db: {
                    tables: []
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Table",
                            table: {
                                filePath: "order_type.sql",
                                identify: "public.order_type",
                                name: "order_type",
                                parsed: null,
                                deprecatedColumns: [],
                                deprecated: false,
                                uniqueConstraints: [],
                                foreignKeysConstraints: [],
                                checkConstraints: [],
                                columns: [
                                    {
                                        filePath: null,
                                        identify: "id",
                                        key: "id",
                                        parsed: null,
                                        nulls: true,
                                        type: "integer"
                                    },
                                    {
                                        filePath: null,
                                        identify: "name",
                                        key: "name",
                                        parsed: null,
                                        nulls: true,
                                        type: "text"
                                    }
                                ],
                                primaryKey: ["id"],
                                values: [
                                    ["1", "FCL"]
                                ]
                            }
                        },
                        {
                            type: "create",
                            command: "Rows",
                            table: {
                                filePath: "order_type.sql",
                                identify: "public.order_type",
                                name: "order_type",
                                parsed: null,
                                deprecatedColumns: [],
                                deprecated: false,
                                uniqueConstraints: [],
                                foreignKeysConstraints: [],
                                checkConstraints: [],
                                columns: [
                                    {
                                        filePath: null,
                                        identify: "id",
                                        key: "id",
                                        parsed: null,
                                        nulls: true,
                                        type: "integer"
                                    },
                                    {
                                        filePath: null,
                                        identify: "name",
                                        key: "name",
                                        parsed: null,
                                        nulls: true,
                                        type: "text"
                                    }
                                ],
                                primaryKey: ["id"],
                                values: [
                                    ["1", "FCL"]
                                ]
                            },
                            values: [
                                ["1", "FCL"]
                            ]
                        }
                    ],
                    errors: []
                }
            });
        });

    });

});
