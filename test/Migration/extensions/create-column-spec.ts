import {testGenerateMigration} from "../testGenerateMigration";
import { columnID, columnNAME, column, extension, table } from "../fixtures/tables";

describe("Migration: extensions", () => {

    describe("create column", () => {
        
        it("create column for existent table by extension", () => {
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
                        table("companies", columnID)
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Column",
                            tableIdentify: "public.companies",
                            column: {
                                filePath: null,
                                identify: "name",
                                key: "name",
                                default: null,
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

        it("create two columns from two extensions", () => {
            testGenerateMigration({
                fs: {
                    extensions: [
                        extension("name", "companies", {
                            columns: [
                                columnNAME
                            ]
                        }),
                        extension("inn", "companies", {
                            columns: [
                                column("inn", "text")
                            ]
                        })
                    ],
                    tables: [
                        table("companies", columnID)
                    ]
                },
                db: {
                    tables: [
                        table("companies", columnID)
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Column",
                            tableIdentify: "public.companies",
                            column: {
                                filePath: null,
                                identify: "name",
                                key: "name",
                                default: null,
                                parsed: null,
                                nulls: true,
                                type: "text"
                            }
                        },
                        {
                            type: "create",
                            command: "Column",
                            tableIdentify: "public.companies",
                            column: {
                                filePath: null,
                                identify: "inn",
                                key: "inn",
                                default: null,
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

    });

});
