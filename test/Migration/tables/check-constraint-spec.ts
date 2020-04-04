import {testGenerateMigration} from "../testGenerateMigration";
import { table, columnID, columnNAME } from "../fixtures/tables";

describe("Migration: tables", () => {

    describe("check constraints", () => {
        const company = table("company", 
            columnID,
            columnNAME
        );
        
        it("create check constraint", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...company,
                        checkConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                check: "true"
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
                            command: "CheckConstraint",
                            tableIdentify: "public.company",
                            constraint: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                check: "true",
                                parsed: null
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("drop check constraint", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        company
                    ]
                },
                db: {
                    tables: [{
                        ...company,
                        checkConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                check: "true"
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "CheckConstraint",
                            tableIdentify: "public.company",
                            constraint: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                check: "true",
                                parsed: null
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("change check constraint (drop/create)", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        ...company,
                        checkConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                check: "true"
                            }
                        ]
                    }]
                },
                db: {
                    tables: [{
                        ...company,
                        checkConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                check: "false"
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "CheckConstraint",
                            tableIdentify: "public.company",
                            constraint: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                check: "false"
                            }
                        },
                        {
                            type: "create",
                            command: "CheckConstraint",
                            tableIdentify: "public.company",
                            constraint: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                check: "true"
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("same check constraint, nothing to do", () => {
            const checkConstraint = {
                identify: "name",
                name: "name",
                check: "true"
            };

            testGenerateMigration({
                fs: {
                    tables: [{
                        ...company,
                        checkConstraints: [
                            checkConstraint
                        ]
                    }]
                },
                db: {
                    tables: [{
                        ...company,
                        checkConstraints: [
                            checkConstraint
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
