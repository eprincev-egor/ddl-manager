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
                                parsed: "xxx"
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
                                parsed: "xxx"
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
                                parsed: "xxx"
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
                                parsed: "xxx"
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
                                parsed: "yyy"
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
                                parsed: "xxx"
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
                                parsed: "xxx"
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
                                parsed: "yyy"
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
                parsed: "xxx"
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
