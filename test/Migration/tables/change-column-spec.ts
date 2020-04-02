import {testGenerateMigration} from "../testGenerateMigration";
import { table, column } from "../fixtures/tables";

describe("Migration: tables", () => {

    describe("change column", () => {
        
        it("error on change column type", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", column("id", "date"))
                    ]
                },
                db: {
                    tables: [
                        table("company", column("id", "integer"))
                    ]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "company.sql",
                            code: "CannotChangeColumnTypeError",
                            message: `cannot change column type public.company.id from integer to date`,
                            tableIdentify: "public.company",
                            columnKey: "id",
                            oldType: "integer",
                            newType: "date"
                        }
                    ]
                }
            });
        });

        it("drop not null for column", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", 
                            column("id", "integer", {
                                nulls: true
                            })
                        )
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            column("id", "integer", {
                                nulls: false
                            })
                        )
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "ColumnNotNull",
                            tableIdentify: "public.company",
                            columnIdentify: "id"
                        }
                    ],
                    errors: []
                }
            });
        });
        
        
        
    });

});
