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
                                    parsed: "xxx"
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
                    extensions: [
                        extension("test", "company", {
                            checkConstraints: [
                                {
                                    identify: "name",
                                    name: "name",
                                    parsed: "yyy"
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
