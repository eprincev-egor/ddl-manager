import testGenerateMigration from "./testGenerateMigration";
import State from "../../lib/State";
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
                                columns: [{
                                    filePath: null,
                                    identify: "id",
                                    key: "id",
                                    parsed: null,
                                    nulls: true,
                                    type: "integer"
                                }],
                                rows: null
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
                    const state = new State({
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
                                columns: [{
                                    filePath: null,
                                    identify: "id",
                                    key: "id",
                                    parsed: null,
                                    nulls: true,
                                    type: "integer"
                                }],
                                rows: null
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
                        rows: [
                            {id: 1, name: "FCL"}
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
                        rows: [
                            {id: 1, name: "FCL"}
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
                        rows: [
                            {id: 1, name: "FCL"}
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
                                rows: [
                                    {id: 1, name: "FCL"}
                                ]
                            },
                            rows: [
                                {id: 1, name: "FCL"}
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
                        rows: [
                            {id: 1, name: "FCL"}
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
                                rows: [
                                    {id: 1, name: "FCL"}
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
                                rows: [
                                    {id: 1, name: "FCL"}
                                ]
                            },
                            rows: [
                                {id: 1, name: "FCL"}
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
        
    });

});
