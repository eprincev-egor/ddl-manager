import {testGenerateMigration} from "../testGenerateMigration";
import { table, columnID, columnNAME, column } from "../fixtures/tables";

describe("Migration: tables", () => {

    describe("create column", () => {
        
        it("create column", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", columnID, columnNAME)
                    ]
                },
                db: {
                    tables: [
                        table("company", columnID)
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Column",
                            tableIdentify: "public.company",
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

        it("create not null for column", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", 
                            column("id", "integer", {
                                nulls: false
                            })
                        )
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            column("id", "integer", {
                                nulls: true
                            })
                        )
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
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
