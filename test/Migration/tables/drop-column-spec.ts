import {testGenerateMigration} from "../testGenerateMigration";
import { table, columnID, columnNAME } from "../fixtures/tables";

describe("Migration: tables", () => {

    describe("drop column", () => {
        
        it("error on drop column, dev database", () => {
            testGenerateMigration({
                options: {
                    mode: "dev"
                },
                fs: {
                    tables: [
                        table("company", columnID)
                    ]
                },
                db: {
                    tables: [
                        table("company", columnID, columnNAME)
                    ]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "company.sql",
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
                        ...table("company", columnID),
                        deprecatedColumns: ["name"]
                    }]
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

        it("no error on drop column, prod database", () => {
            testGenerateMigration({
                options: {
                    mode: "prod"
                },
                fs: {
                    tables: [
                        table("company", columnID)
                    ]
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

        it("no error on drop column, prod database (default behavior)", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", columnID)
                    ]
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

        it("don't drop deprecated column", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...table("company", columnID),
                        deprecatedColumns: ["name"]
                    }]
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
