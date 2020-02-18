import testGenerateMigration from "./testGenerateMigration";
import State from "../../lib/State";
import assert from "assert";

describe("State", () => {

    describe("generateMigration for tables", () => {
        
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
                            table: {
                                filePath: null,
                                identify: "public.company",
                                name: "company",
                                parsed: null,
                                deprecatedColumns: [],
                                deprecated: false,
                                columns: [{
                                    filePath: null,
                                    identify: "id",
                                    key: "id",
                                    parsed: null,
                                    type: "integer"
                                }]
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
                            tableIdentify: "public.company",
                            column: {
                                filePath: null,
                                identify: "name",
                                key: "name",
                                parsed: null,
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
                            table: {
                                filePath: null,
                                deprecated: false,
                                identify: "public.company",
                                name: "company",
                                parsed: null,
                                deprecatedColumns: ["name"],
                                columns: [{
                                    filePath: null,
                                    identify: "id",
                                    key: "id",
                                    parsed: null,
                                    type: "integer"
                                }]
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
        
    });

});
