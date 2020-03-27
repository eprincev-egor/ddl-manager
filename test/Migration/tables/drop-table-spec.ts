import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: tables", () => {

    describe("generateTables", () => {
        
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
        
    });

});
