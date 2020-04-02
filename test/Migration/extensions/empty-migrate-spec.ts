import {testGenerateMigration} from "../testGenerateMigration";
import { extension, table, columnNAME, columnID } from "../fixtures/tables";

describe("Migration: extensions", () => {

    describe("empty migration", () => {
        
        it("empty migration for same db and fs state (extension with column)", () => {
            testGenerateMigration({
                fs: {
                    extensions: [
                        extension("test", "companies", {
                            columns: [
                                columnNAME
                            ]
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
                    errors: []
                }
            });
        });

    });
    
});
