import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: tables", () => {

    describe("create table", () => {
        
        it("create table for empty db", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }]
                },
                db: {
                    tables: []
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Table",
                            table: {
                                filePath: null,
                                identify: "public.company",
                                name: "company",
                                parsed: null,
                                deprecatedColumns: [],
                                deprecated: false,
                                primaryKey: null,
                                uniqueConstraints: [],
                                foreignKeysConstraints: [],
                                checkConstraints: [],
                                columns: [{
                                    filePath: null,
                                    identify: "id",
                                    key: "id",
                                    default: null,
                                    parsed: null,
                                    nulls: true,
                                    type: "integer"
                                }],
                                values: null
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("create table for empty db, without deprecated columns", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }],
                        deprecatedColumns: ["name"]
                    }]
                },
                db: {
                    tables: []
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Table",
                            table: {
                                filePath: null,
                                deprecated: false,
                                identify: "public.company",
                                name: "company",
                                parsed: null,
                                deprecatedColumns: ["name"],
                                primaryKey: null,
                                uniqueConstraints: [],
                                foreignKeysConstraints: [],
                                checkConstraints: [],
                                columns: [{
                                    filePath: null,
                                    identify: "id",
                                    key: "id",
                                    default: null,
                                    parsed: null,
                                    nulls: true,
                                    type: "integer"
                                }],
                                values: null
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
             
        it("don't create deprecated table", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        deprecated: true,
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }]
                },
                db: {
                    tables: []
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });
        
    });

});
