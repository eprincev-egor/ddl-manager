import {testGenerateMigration} from "../testGenerateMigration";
import { table, columnID, columnNAME, extension } from "../fixtures/tables";

describe("Migration: extensions", () => {

    describe("drop column", () => {
        
        it("error on drop column, dev database", () => {
            testGenerateMigration({
                options: {
                    mode: "dev"
                },
                fs: {
                    extensions: [
                        extension("test", "companies", {
                            columns: []
                        })
                    ],
                    tables: [
                        table("companies", columnID)
                    ]
                },
                db: {
                    tables: [
                        table("companies", columnID, columnNAME)
                    ]
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
