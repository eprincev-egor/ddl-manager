import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: tables", () => {

    describe("change column", () => {
        
        it("error on change column type", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "my_table.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "date"
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "my_table.sql",
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
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer",
                            nulls: true
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer",
                            nulls: false
                        }]
                    }]
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
