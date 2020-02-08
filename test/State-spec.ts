import testGenerateMigration from "./testGenerateMigration";

describe("State", () => {

    describe("generateMigration", () => {
        
        it("create function for empty db", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        schema: "public",
                        name: "test"
                    }]
                },
                db: {
                    functions: []
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            function: {
                                schema: "public",
                                name: "test",
                                args: ""
                            }
                        }
                    ]
                }
            });
        });

        it("remove function for db with one function", () => {
            testGenerateMigration({
                fs: {
                    functions: []
                },
                db: {
                    functions: [{
                        schema: "public",
                        name: "test"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            function: {
                                schema: "public",
                                name: "test",
                                args: ""
                            }
                        }
                    ]
                }
            });
        });

        it("db and fs has only one function, empty migration", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        schema: "public",
                        name: "test"
                    }]
                },
                db: {
                    functions: [{
                        schema: "public",
                        name: "test"
                    }]
                },
                migration: {
                    commands: []
                }
            });
        });

        it("db and fs has only one function, but that different functions", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        schema: "public",
                        name: "test1"
                    }]
                },
                db: {
                    functions: [{
                        schema: "public",
                        name: "test2"
                    }]
                },
                migration: {
                    commands: [
                        // first the 'drop'
                        {
                            type: "drop",
                            function: {
                                schema: "public",
                                name: "test2",
                                args: ""
                            }
                        },
                        // second the 'create'
                        {
                            type: "create",
                            function: {
                                schema: "public",
                                name: "test1",
                                args: ""
                            }
                        }
                    ]
                }
            });
        });

        it("create view for empty db", () => {
            testGenerateMigration({
                fs: {
                    views: [{
                        schema: "public",
                        name: "operations_view"
                    }]
                },
                db: {
                    views: []
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            view: {
                                schema: "public",
                                name: "operations_view"
                            }
                        }
                    ]
                }
            });
        });

        
        it("remove view for db with one view", () => {
            testGenerateMigration({
                fs: {
                    views: []
                },
                db: {
                    views: [{
                        schema: "public",
                        name: "operations_view"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            view: {
                                schema: "public",
                                name: "operations_view"
                            }
                        }
                    ]
                }
            });
        });

        it("create table for empty db", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        schema: "public",
                        name: "company",
                        columns: [{
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
                                schema: "public",
                                name: "company",
                                columns: [{
                                    key: "id",
                                    type: "integer"
                                }]
                            }
                        }
                    ]
                }
            });
        });

        it("create column", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        schema: "public",
                        name: "company",
                        columns: [{
                            key: "id",
                            type: "integer"
                        }, {
                            key: "name",
                            type: "text"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        schema: "public",
                        name: "company",
                        columns: [{
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            schema: "public",
                            table: "company",
                            column: {
                                key: "name",
                                type: "text"
                            }
                        }
                    ]
                }
            });
        });

        
        it("db and fs has only one same table, empty migration", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        schema: "public",
                        name: "company",
                        columns: [{
                            key: "id",
                            type: "integer"
                        }, {
                            key: "name",
                            type: "text"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        schema: "public",
                        name: "company",
                        columns: [{
                            key: "id",
                            type: "integer"
                        }, {
                            key: "name",
                            type: "text"
                        }]
                    }]
                },
                migration: {
                    commands: []
                }
            });
        });

        
        it("create trigger", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        schema: "public",
                        name: "company",
                        columns: [{
                            key: "id",
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        schema: "public",
                        name: "create_role",
                        args: ""
                    }],
                    triggers: [{
                        schema: "public",
                        table: "company",
                        name: "create_role_trigger"
                    }]
                },
                db: {
                    tables: [{
                        schema: "public",
                        name: "company",
                        columns: [{
                            key: "id",
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        schema: "public",
                        name: "create_role",
                        args: ""
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            trigger: {
                                schema: "public",
                                table: "company",
                                name: "create_role_trigger"
                            }
                        }
                    ]
                }
            });
        });
        
        it("drop trigger", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        schema: "public",
                        name: "company",
                        columns: [{
                            key: "id",
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        schema: "public",
                        name: "create_role",
                        args: ""
                    }]
                },
                db: {
                    tables: [{
                        schema: "public",
                        name: "company",
                        columns: [{
                            key: "id",
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        schema: "public",
                        name: "create_role",
                        args: ""
                    }],
                    triggers: [{
                        schema: "public",
                        table: "company",
                        name: "create_role_trigger"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            trigger: {
                                schema: "public",
                                table: "company",
                                name: "create_role_trigger"
                            }
                        }
                    ]
                }
            });
        });
    });
    
});
