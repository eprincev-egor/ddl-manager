import {testGenerateMigration} from "../testGenerateMigration";
import { table, columnID, columnNAME } from "../fixtures/tables";

describe("Migration: tables", () => {

    describe("unique constraint", () => {

        const company = table("company", columnID, columnNAME);
        
        it("create unique constraint", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...company,
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }]
                },
                db: {
                    tables: [
                        company
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "UniqueConstraint",
                            tableIdentify: "public.company",
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
        

        it("drop unique constraint", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        company
                    ]
                },
                db: {
                    tables: [{
                        ...company,
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
                            tableIdentify: "public.company",
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

        
        it("change unique constraint (drop/create)", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...company,
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["id"]
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        ...company,
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
                            tableIdentify: "public.company",
                            unique: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                unique: ["name"]
                            }
                        },
                        {
                            type: "create",
                            command: "UniqueConstraint",
                            tableIdentify: "public.company",
                            unique: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                unique: ["id"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("same unique constraint, nothing to do", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...company,
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        ...company,
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
                    commands: [],
                    errors: []
                }
            });
        });

    });

});
