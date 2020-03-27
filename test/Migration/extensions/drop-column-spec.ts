import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: extensions", () => {

    describe("drop column", () => {
        
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
