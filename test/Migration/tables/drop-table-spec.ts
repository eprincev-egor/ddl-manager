import {testGenerateMigration} from "../testGenerateMigration";
import { table, columnID, columnNAME } from "../fixtures/tables";

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
                    tables: [
                        table("company", columnID)
                    ]
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
                    tables: [
                        table("company", columnID, columnNAME)
                    ]
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
                    tables: [
                        table("company", columnID, columnNAME)
                    ]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });
        
    });

});
