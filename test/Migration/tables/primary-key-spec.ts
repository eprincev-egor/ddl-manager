import {testGenerateMigration} from "../testGenerateMigration";
import { table, columnID, columnNAME } from "../fixtures/tables";

describe("Migration: tables", () => {

    describe("primary key", () => {
        
        it("create primary key", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...table("company", columnID),
                        primaryKey: ["id"]
                    }]
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
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id"]
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("drop primary key", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", columnID)
                    ]
                },
                db: {
                    tables: [{
                        ...table("company", columnID),
                        primaryKey: ["id"]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id"]
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("change primary key (other columns)", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...table("company", columnID, columnNAME),
                        primaryKey: ["id"]
                    }]
                },
                db: {
                    tables: [{
                        ...table("company", columnID, columnNAME),
                        primaryKey: ["id", "name"]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id", "name"]
                        },
                        {
                            type: "create",
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id"]
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("same primary key, but another keys sort, nothing to do", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...table("company", columnID, columnNAME),
                        primaryKey: ["name", "id"]
                    }]
                },
                db: {
                    tables: [{
                        ...table("company", columnID, columnNAME),
                        primaryKey: ["id", "name"]
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

    });

});
