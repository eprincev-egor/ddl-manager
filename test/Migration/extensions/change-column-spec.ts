import {testGenerateMigration} from "../testGenerateMigration";
import { table, extension, column, columnID, columnNAME } from "../fixtures/tables";

describe("Migration: extensions", () => {

    describe("change column", () => {
        
        it("error on change column type", () => {
            testGenerateMigration({
                fs: {
                    extensions: [
                        extension("test", "company", {
                            columns: [
                                column("test", "date")
                            ]
                        })
                    ],
                    tables: [
                        table("company", columnID)
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            columnID, 
                            column("test", "integer")
                        )
                    ]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "company.sql",
                            code: "CannotChangeColumnTypeError",
                            message: `cannot change column type public.company.test from integer to date`,
                            tableIdentify: "public.company",
                            columnKey: "test",
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
                    extensions: [
                        extension("test", "company", {
                            columns: [
                                column("note", "text", {
                                    nulls: true
                                })
                            ]
                        })
                    ],
                    tables: [
                        table("company", columnID)
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            columnID, 
                            column("note", "text", {
                                nulls: false
                            })
                        )
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "ColumnNotNull",
                            tableIdentify: "public.company",
                            columnIdentify: "note"
                        }
                    ],
                    errors: []
                }
            });
        });
        
        
        it("change column, who exists inside table", () => {
            testGenerateMigration({
                fs: {
                    extensions: [
                        extension("test", "company", {
                            columns: [
                                column("name", "text", {
                                    nulls: false
                                })
                            ]
                        })
                    ],
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
                    commands: [
                        {
                            type: "create",
                            command: "ColumnNotNull",
                            tableIdentify: "public.company",
                            columnIdentify: "name"
                        }
                    ],
                    errors: []
                }
            });
        });
        
        
        it("change default expression", () => {
            testGenerateMigration({
                fs: {
                    extensions: [
                        extension("test", "company", {
                            columns: [
                                column("name", "text", {
                                    default: "fs"
                                })
                            ]
                        })
                    ],
                    tables: [
                        table("company", columnID)
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            columnID,
                            column("name", "text", {
                                default: "db"
                            })
                        )
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "ColumnDefault",
                            tableIdentify: "public.company",
                            columnIdentify: "name",
                            default: "db"
                        },
                        {
                            type: "create",
                            command: "ColumnDefault",
                            tableIdentify: "public.company",
                            columnIdentify: "name",
                            default: "fs"
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("only create default expression", () => {
            testGenerateMigration({
                fs: {
                    extensions: [
                        extension("test", "company", {
                            columns: [
                                column("name", "text", {
                                    default: "fs"
                                })
                            ]
                        })
                    ],
                    tables: [
                        table("company", columnID)
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            columnID,
                            columnNAME
                        )
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "ColumnDefault",
                            tableIdentify: "public.company",
                            columnIdentify: "name",
                            default: "fs"
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("only drop default expression", () => {
            testGenerateMigration({
                fs: {
                    extensions: [
                        extension("test", "company", {
                            columns: [
                                columnNAME
                            ]
                        })
                    ],
                    tables: [
                        table("company", columnID)
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            columnID,
                            column("name", "text", {
                                default: "db"
                            })
                        )
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "ColumnDefault",
                            tableIdentify: "public.company",
                            columnIdentify: "name",
                            default: "db"
                        }
                    ],
                    errors: []
                }
            });
        });
    });

});
