import {testGenerateMigration} from "../testGenerateMigration";
import { table, columnID, columnNAME, extension } from "../fixtures/tables";

describe("Migration: extensions", () => {

    describe("check constraints", () => {
        const company = table("company", 
            columnID,
            columnNAME
        );
        
        it("create check constraint", () => {
            testGenerateMigration({
                fs: {
                    extensions: [
                        extension("test", "company", {
                            checkConstraints: [
                                {
                                    identify: "name",
                                    name: "name",
                                    check: "true"
                                }
                            ]
                        })
                    ],
                    tables: [
                        company
                    ]
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
                                parsed: null,
                                check: "true"
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
                    extensions: [
                        extension("test", "company", {
                            checkConstraints: []
                        })
                    ],
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
                                parsed: null,
                                check: "true"
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
                    extensions: [
                        extension("test", "company", {
                            checkConstraints: [
                                {
                                    identify: "name",
                                    name: "name",
                                    check: "true"
                                }
                            ]
                        })
                    ],
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
                                check: "false",
                                parsed: null
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
                    extensions: [
                        extension("test", "company", {
                            checkConstraints: [
                                checkConstraint
                            ]
                        })
                    ],
                    tables: [
                        company
                    ]
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
