import testGenerateMigration from "./testGenerateMigration";
import DDLState from "../../lib/state/DDLState";
import assert from "assert";

describe("MigrationController", () => {

    describe("generateTables", () => {
        
        it("create table for empty db", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
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
                                filePath: null,
                                identify: "public.company",
                                name: "company",
                                parsed: null,
                                deprecatedColumns: [],
                                deprecated: false,
                                primaryKey: null,
                                uniqueConstraints: [],
                                foreignKeysConstraints: [],
                                checkConstraints: [],
                                columns: [{
                                    filePath: null,
                                    identify: "id",
                                    key: "id",
                                    parsed: null,
                                    nulls: true,
                                    type: "integer"
                                }],
                                values: null
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("create column", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Column",
                            tableIdentify: "public.company",
                            column: {
                                filePath: null,
                                identify: "name",
                                key: "name",
                                parsed: null,
                                nulls: true,
                                type: "text"
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("db and fs has only one same table, empty migration", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

        

        it("maximum table name size is 64 symbols", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.abcd012345678901234567890123456789012345678901234567890123456789_tail",
                        name: "abcd012345678901234567890123456789012345678901234567890123456789_tail",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                db: {
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "my_table.sql",
                            code: "MaxObjectNameSizeError",
                            message: "table name too long: abcd012345678901234567890123456789012345678901234567890123456789_tail, max size is 64 symbols",
                            name: "abcd012345678901234567890123456789012345678901234567890123456789_tail",
                            objectType: "table"
                        }
                    ]
                }
            });
        });

        
        it("error on drop column, dev database", () => {
            testGenerateMigration({
                options: {
                    mode: "dev"
                },
                fs: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
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
                            filePath: "my_table.sql",
                            code: "CannotDropColumnError",
                            message: "cannot drop column public.company.name, please use deprecated section",
                            tableIdentify: "public.company",
                            columnKey: "name"
                        }
                    ]
                }
            });
        });

        it("no error if: fs has deprecated column, db has actual column", () => {
            testGenerateMigration({
                options: {
                    mode: "dev"
                },
                fs: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }
                        ],
                        deprecatedColumns: ["name"]
                    }]
                },
                db: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
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
                    errors: []
                }
            });
        });

        it("no error on drop column, prod database", () => {
            testGenerateMigration({
                options: {
                    mode: "prod"
                },
                fs: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
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
                    errors: []
                }
            });
        });

        it("no error on drop column, prod database (default behavior)", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
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
                    errors: []
                }
            });
        });

        
        it("columns should be only actual or only deprecated", () => {
            assert.throws(
                () => {
                    const state = new DDLState({
                        tables: [{
                            filePath: "my_table.sql",
                            identify: "public.company",
                            name: "company",
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
                            deprecatedColumns: ["id", "name"]
                        }]
                    });
                },
                (err) =>
                    err.message === "columns should be only actual or only deprecated: id,name"
            );
        });


        it("create table for empty db, without deprecated columns", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }],
                        deprecatedColumns: ["name"]
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
                                filePath: null,
                                deprecated: false,
                                identify: "public.company",
                                name: "company",
                                parsed: null,
                                deprecatedColumns: ["name"],
                                primaryKey: null,
                                uniqueConstraints: [],
                                foreignKeysConstraints: [],
                                checkConstraints: [],
                                columns: [{
                                    filePath: null,
                                    identify: "id",
                                    key: "id",
                                    parsed: null,
                                    nulls: true,
                                    type: "integer"
                                }],
                                values: null
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
             

        it("don't drop deprecated column", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }],
                        deprecatedColumns: ["name"]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });
        
        it("error on drop table, dev database", () => {
            testGenerateMigration({
                options: {
                    mode: "dev"
                },
                fs: {
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "(database)",
                            code: "CannotDropTableError",
                            message: "cannot drop table public.company, please use deprecated keyword before table definition",
                            tableIdentify: "public.company"
                        }
                    ]
                }
            });
        });

        
        it("no error on drop table, prod database", () => {
            testGenerateMigration({
                options: {
                    mode: "prod"
                },
                fs: {
                },
                db: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
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
                    errors: []
                }
            });
        });
        
        it("no error on drop table, prod database (default behavior)", () => {
            testGenerateMigration({
                options: {
                    mode: "prod"
                },
                fs: {
                },
                db: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
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
                    errors: []
                }
            });
        });
        

        it("error on change column type", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "date"
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "my_table.sql",
                            code: "CannotChangeColumnTypeError",
                            message: `cannot change column type public.company.id from integer to date`,
                            tableIdentify: "public.company",
                            columnKey: "id",
                            oldType: "integer",
                            newType: "date"
                        }
                    ]
                }
            });
        });

        it("don't create deprecated table", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        deprecated: true,
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                db: {
                    tables: []
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });
        
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

        it("drop not null for column", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer",
                            nulls: true
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer",
                            nulls: false
                        }]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "ColumnNotNull",
                            tableIdentify: "public.company",
                            columnIdentify: "id"
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("create not null for column", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer",
                            nulls: false
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer",
                            nulls: true
                        }]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "ColumnNotNull",
                            tableIdentify: "public.company",
                            columnIdentify: "id"
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("create primary key", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }],
                        primaryKey: ["id"]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id"]
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("drop primary key", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }],
                        primaryKey: ["id"]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id"]
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("change primary key (other columns)", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        primaryKey: ["id"]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        primaryKey: ["id", "name"]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id", "name"]
                        },
                        {
                            type: "create",
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id"]
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("same primary key, but another keys sort, nothing to do", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        primaryKey: ["name", "id"]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        primaryKey: ["id", "name"]
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

        it("create check constraint", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
                        checkConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                parsed: "xxx"
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "CheckConstraint",
                            tableIdentify: "public.company",
                            constraint: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: "xxx"
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("drop check constraint", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
                        checkConstraints: []
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
                        checkConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                parsed: "xxx"
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "CheckConstraint",
                            tableIdentify: "public.company",
                            constraint: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: "xxx"
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("change check constraint (drop/create)", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
                        checkConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                parsed: "yyy"
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
                        checkConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                parsed: "xxx"
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "CheckConstraint",
                            tableIdentify: "public.company",
                            constraint: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: "xxx"
                            }
                        },
                        {
                            type: "create",
                            command: "CheckConstraint",
                            tableIdentify: "public.company",
                            constraint: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: "yyy"
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("same check constraint, nothing to do", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
                        checkConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                parsed: "xxx"
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
                        checkConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                parsed: "xxx"
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

        
        
        it("create unique constraint", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "UniqueConstraint",
                            tableIdentify: "public.company",
                            unique: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                unique: ["name"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        

        it("drop unique constraint", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "UniqueConstraint",
                            tableIdentify: "public.company",
                            unique: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                unique: ["name"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("change unique constraint (drop/create)", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["id"]
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "UniqueConstraint",
                            tableIdentify: "public.company",
                            unique: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                unique: ["name"]
                            }
                        },
                        {
                            type: "create",
                            command: "UniqueConstraint",
                            tableIdentify: "public.company",
                            unique: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                unique: ["id"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("same unique constraint, nothing to do", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

        it("create foreign key", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"]
                                }
                            ]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "ForeignKeyConstraint",
                            tableIdentify: "public.company",
                            foreignKey: {
                                filePath: null,
                                parsed: null,
                                identify: "country_fk",
                                name: "country_fk",
                                columns: ["id_country"],
                                referenceTableIdentify: "public.country",
                                referenceColumns: ["id"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("drop foreign key", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"]
                                }
                            ]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "ForeignKeyConstraint",
                            tableIdentify: "public.company",
                            foreignKey: {
                                filePath: null,
                                parsed: null,
                                identify: "country_fk",
                                name: "country_fk",
                                columns: ["id_country"],
                                referenceTableIdentify: "public.country",
                                referenceColumns: ["id"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("change foreign key (drop/create)", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"],
                                    parsed: "xxx"
                                }
                            ]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"],
                                    parsed: "yyy"
                                }
                            ]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "ForeignKeyConstraint",
                            tableIdentify: "public.company",
                            foreignKey: {
                                filePath: null,
                                parsed: "yyy",
                                identify: "country_fk",
                                name: "country_fk",
                                columns: ["id_country"],
                                referenceTableIdentify: "public.country",
                                referenceColumns: ["id"]
                            }
                        },
                        {
                            type: "create",
                            command: "ForeignKeyConstraint",
                            tableIdentify: "public.company",
                            foreignKey: {
                                filePath: null,
                                parsed: "xxx",
                                identify: "country_fk",
                                name: "country_fk",
                                columns: ["id_country"],
                                referenceTableIdentify: "public.country",
                                referenceColumns: ["id"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });


        it("same foreign key, nothing to do", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"]
                                }
                            ]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"]
                                }
                            ]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

        it("foreign key reference to unknown table", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country2",
                                    referenceColumns: ["id"]
                                }
                            ]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            code: "ReferenceToUnknownTableError",
                            filePath: "company.sql",
                            foreignKeyName: "country_fk",
                            message: "foreign key 'country_fk' on table 'public.company' reference to unknown table 'public.country2'",
                            referenceTableIdentify: "public.country2",
                            tableIdentify: "public.company"
                        }
                    ]
                }
            });
        });


        it("foreign key reference to unknown column", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id2"]
                                }
                            ]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            code: "ReferenceToUnknownColumnError",
                            filePath: "company.sql",
                            foreignKeyName: "country_fk",
                            message: "foreign key 'country_fk' on table 'public.company' reference to unknown columns 'id2' in table 'public.country'",
                            referenceTableIdentify: "public.country",
                            referenceColumns: ["id2"],
                            tableIdentify: "public.company"
                        }
                    ]
                }
            });
        });

    });

});
