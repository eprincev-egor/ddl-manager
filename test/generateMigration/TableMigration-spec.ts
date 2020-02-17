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
                            key: "id"
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
                                columns: [{
                                    filePath: null,
                                    identify: "id",
                                    key: "id",
                                    parsed: null
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
                            key: "id"
                        }, {
                            identify: "name",
                            key: "name"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id"
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
                                parsed: null
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
                            key: "id"
                        }, {
                            identify: "name",
                            key: "name"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id"
                        }, {
                            identify: "name",
                            key: "name"
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
                            key: "id"
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

        
        it("error on drop column", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id"
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
                                key: "id"
                            },
                            {
                                identify: "name",
                                key: "name"
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
                                    key: "id"
                                },
                                {
                                    identify: "name",
                                    key: "name"
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
                            key: "id"
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
                                identify: "public.company",
                                name: "company",
                                parsed: null,
                                deprecatedColumns: ["name"],
                                columns: [{
                                    filePath: null,
                                    identify: "id",
                                    key: "id",
                                    parsed: null
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
                            key: "id"
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
                            key: "id"
                        }, {
                            identify: "name",
                            key: "name"
                        }]
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
