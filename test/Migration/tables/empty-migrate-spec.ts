import {testGenerateMigration} from "../testGenerateMigration";
import { table, columnID, columnNAME } from "../fixtures/tables";

describe("Migration: tables", () => {

    describe("generateTables", () => {
        
        it("db and fs has only one same table, empty migration", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", columnID, columnNAME)
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
        
    });

});
