import {testGenerateMigration} from "../testGenerateMigration";
import { columnNAME, extension, table, columnID } from "../fixtures/tables";

describe("Migration: extensions", () => {

    describe("unique constraints", () => {
        
        it("create unique constraint from extension", () => {
            testGenerateMigration({
                fs: {
                    extensions: [
                        extension("test", "companies", {
                            columns: [
                                columnNAME
                            ],
                            uniqueConstraints: [
                                {
                                    identify: "name",
                                    name: "name",
                                    unique: ["name"]
                                }
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
                    commands: [
                        {
                            type: "create",
                            command: "UniqueConstraint",
                            tableIdentify: "public.companies",
                            unique: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                unique: ["name"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("drop unique constraint from extension", () => {
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
                    tables: [{
                        ...table("companies", columnID, columnNAME),
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "UniqueConstraint",
                            tableIdentify: "public.companies",
                            unique: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                unique: ["name"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        
    });

});
