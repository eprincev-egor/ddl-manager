import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: tables", () => {

    describe("create column", () => {
        
        it("create column", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
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
                            type: "integer"
                        }]
                    }]
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
                db: {
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
