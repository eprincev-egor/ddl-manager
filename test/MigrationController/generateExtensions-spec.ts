import testGenerateMigration from "./testGenerateMigration";

describe("MigrationController", () => {

    describe("generateExtensions", () => {
        
        it("error on unknown table", () => {
            testGenerateMigration({
                fs: {
                    extensions: [{
                        filePath: "test_for_companies.sql",
                        name: "test",
                        identify: "extension test for public.companies",
                        forTableIdentify: "public.companies"
                    }]
                },
                db: {
                    functions: []
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "test_for_companies.sql",
                            code: "UnknownTableForExtensionError",
                            message: "table public.companies does not exists for extension test",
                            extensionName: "test",
                            tableIdentify: "public.companies"
                        }
                    ]
                }
            });
        });

        it("create column for existent table by extension", () => {
            testGenerateMigration({
                fs: {
                    extensions: [{
                        filePath: "test_for_companies.sql",
                        name: "test",
                        identify: "extension test for public.companies",
                        forTableIdentify: "public.companies",
                        columns: [{
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }],
                    tables: [{
                        filePath: "companies.sql",
                        identify: "public.companies",
                        name: "companies",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.companies",
                        name: "companies",
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
                            tableIdentify: "public.companies",
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

        it("empty migration for same db and fs state (extension with column)", () => {
            testGenerateMigration({
                fs: {
                    extensions: [{
                        filePath: "test_for_companies.sql",
                        name: "test",
                        identify: "extension test for public.companies",
                        forTableIdentify: "public.companies",
                        columns: [{
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }],
                    tables: [{
                        filePath: "companies.sql",
                        identify: "public.companies",
                        name: "companies",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.companies",
                        name: "companies",
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

        it("error on drop column, dev database", () => {
            testGenerateMigration({
                options: {
                    mode: "dev"
                },
                fs: {
                    extensions: [{
                        filePath: "test_for_companies.sql",
                        name: "test",
                        identify: "extension test for public.companies",
                        forTableIdentify: "public.companies",
                        columns: []
                    }],
                    tables: [{
                        filePath: "companies.sql",
                        identify: "public.companies",
                        name: "companies",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.companies",
                        name: "companies",
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
                            filePath: "companies.sql",
                            code: "CannotDropColumnError",
                            message: "cannot drop column public.companies.name, please use deprecated section",
                            tableIdentify: "public.companies",
                            columnKey: "name"
                        }
                    ]
                }
            });
        });

    });
    
});
