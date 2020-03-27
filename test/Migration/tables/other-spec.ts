import {testGenerateMigration} from "../testGenerateMigration";
import {DDLState} from "../../../lib/state/DDLState";
import assert from "assert";

describe("Migration: tables", () => {

    describe("other", () => {
        
        it("maximum table name size is 64 symbols", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.abcd012345678901234567890123456789012345678901234567890123456789_tail",
                        name: "abcd012345678901234567890123456789012345678901234567890123456789_tail",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                db: {
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "my_table.sql",
                            code: "MaxObjectNameSizeError",
                            message: "table name too long: abcd012345678901234567890123456789012345678901234567890123456789_tail, max size is 64 symbols",
                            name: "abcd012345678901234567890123456789012345678901234567890123456789_tail",
                            objectType: "table"
                        }
                    ]
                }
            });
        });

        it("columns should be only actual or only deprecated", () => {
            assert.throws(
                () => {
                    const state = new DDLState({
                        tables: [{
                            filePath: "my_table.sql",
                            identify: "public.company",
                            name: "company",
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
                            ],
                            deprecatedColumns: ["id", "name"]
                        }]
                    });
                },
                (err) =>
                    err.message === "columns should be only actual or only deprecated: id,name"
            );
        });
        
    });

});
