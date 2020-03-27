import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: tables", () => {

    describe("drop column", () => {
        
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
        
    });

});
