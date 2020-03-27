import {testGenerateMigration} from "../testGenerateMigration";

describe("MigrationController", () => {

    describe("generateExtensions", () => {
        
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

    });

});
