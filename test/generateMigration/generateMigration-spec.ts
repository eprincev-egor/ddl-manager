import testGenerateMigration from "./testGenerateMigration";

describe("State", () => {

    describe("generateMigration", () => {
        
        it("create function for empty db", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        identify: "public.test()",
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
                                identify: "public.test()",
                                name: "test",
                                parsed: null
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
                        identify: "public.test()",
                        name: "test"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            function: {
                                identify: "public.test()",
                                name: "test",
                                parsed: null
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
                        identify: "public.test()",
                        name: "test"
                    }]
                },
                db: {
                    functions: [{
                        identify: "public.test()",
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
                        identify: "public.test1()",
                        name: "test1"
                    }]
                },
                db: {
                    functions: [{
                        identify: "public.test2()",
                        name: "test2"
                    }]
                },
                migration: {
                    commands: [
                        // first the 'drop'
                        {
                            type: "drop",
                            function: {
                                identify: "public.test2()",
                                name: "test2",
                                parsed: null
                            }
                        },
                        // second the 'create'
                        {
                            type: "create",
                            function: {
                                identify: "public.test1()",
                                name: "test1",
                                parsed: null
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
                        identify: "public.operations_view",
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
                                identify: "public.operations_view",
                                name: "operations_view",
                                parsed: null
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
                        identify: "public.operations_view",
                        name: "operations_view"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            view: {
                                identify: "public.operations_view",
                                name: "operations_view",
                                parsed: null
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
                                identify: "public.company",
                                name: "company",
                                parsed: null,
                                columns: [{
                                    identify: "id",
                                    key: "id",
                                    parsed: null
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
                                identify: "name",
                                key: "name",
                                parsed: null
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
                    commands: []
                }
            });
        });

        
        it("create trigger", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        name: "create_role_trigger"
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
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            trigger: {
                                identify: "create_role_trigger on public.company",
                                tableIdentify: "public.company",
                                name: "create_role_trigger",
                                parsed: null
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
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
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
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        name: "create_role_trigger"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            trigger: {
                                identify: "create_role_trigger on public.company",
                                tableIdentify: "public.company",
                                name: "create_role_trigger",
                                parsed: null
                            }
                        }
                    ]
                }
            });
        });
    });
    
});
