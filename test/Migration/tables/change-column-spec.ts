import {testGenerateMigration} from "../testGenerateMigration";
import { table, column } from "../fixtures/tables";

describe("Migration: tables", () => {

    describe("change column", () => {
        
        it("error on change column type", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", column("id", "date"))
                    ]
                },
                db: {
                    tables: [
                        table("company", column("id", "integer"))
                    ]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "company.sql",
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
                    tables: [
                        table("company", 
                            column("id", "integer", {
                                nulls: true
                            })
                        )
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            column("id", "integer", {
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
                            columnIdentify: "id"
                        }
                    ],
                    errors: []
                }
            });
        });
        
        
        it("change default expression", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", 
                            column("id", "integer", {
                                default: "2"
                            })
                        )
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            column("id", "integer", {
                                default: "1"
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
                            columnIdentify: "id",
                            default: "1"
                        },
                        {
                            type: "create",
                            command: "ColumnDefault",
                            tableIdentify: "public.company",
                            columnIdentify: "id",
                            default: "2"
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("only create default expression", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", 
                            column("id", "integer", {
                                default: "2"
                            })
                        )
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            column("id", "integer")
                        )
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "ColumnDefault",
                            tableIdentify: "public.company",
                            columnIdentify: "id",
                            default: "2"
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("only drop default expression", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        table("company", 
                            column("id", "integer")
                        )
                    ]
                },
                db: {
                    tables: [
                        table("company", 
                            column("id", "integer", {
                                default: "1"
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
                            columnIdentify: "id",
                            default: "1"
                        }
                    ],
                    errors: []
                }
            });
        });
    });

});
