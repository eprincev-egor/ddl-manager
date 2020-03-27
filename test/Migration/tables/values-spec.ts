import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: tables", () => {

    describe("values (rows)", () => {
        
        it("create rows, expected primary key error", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "order_type.sql",
                        identify: "order_type",
                        name: "order_type",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                identify: "name",
                                key: "name",
                                type: "text"
                            }
                        ],
                        values: [
                            ["1", "FCL"]
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "order_type",
                        name: "order_type",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                identify: "name",
                                key: "name",
                                type: "text"
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "order_type.sql",
                            code: "ExpectedPrimaryKeyForRowsError",
                            message: "table order_type should have primary key for creating rows",
                            tableIdentify: "order_type"
                        }                        
                    ]
                }
            });
        });
        
        it("create rows, expected primary key error, empty db", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "order_type.sql",
                        identify: "order_type",
                        name: "order_type",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                identify: "name",
                                key: "name",
                                type: "text"
                            }
                        ],
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
                            message: "table order_type should have primary key for creating rows",
                            tableIdentify: "order_type"
                        }                        
                    ]
                }
            });
        });
        
        it("create rows, db table exists", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "order_type.sql",
                        identify: "order_type",
                        name: "order_type",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                identify: "name",
                                key: "name",
                                type: "text"
                            }
                        ],
                        primaryKey: ["id"],
                        values: [
                            ["1", "FCL"]
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "order_type",
                        name: "order_type",
                        primaryKey: ["id"],
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                identify: "name",
                                key: "name",
                                type: "text"
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [
                        
                        {
                            type: "create",
                            command: "Rows",
                            table: {
                                filePath: "order_type.sql",
                                identify: "order_type",
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
                        filePath: "order_type.sql",
                        identify: "order_type",
                        name: "order_type",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                identify: "name",
                                key: "name",
                                type: "text"
                            }
                        ],
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
                                identify: "order_type",
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
                                identify: "order_type",
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
