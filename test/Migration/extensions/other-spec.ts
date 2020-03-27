import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: extensions", () => {

    describe("other", () => {
        
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
        
    });

});
