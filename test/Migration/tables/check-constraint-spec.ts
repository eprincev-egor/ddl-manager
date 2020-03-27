import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: tables", () => {

    describe("check constraints", () => {
        
        it("create check constraint", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
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
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }]
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
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
                        checkConstraints: []
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
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
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
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
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
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
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
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
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [],
                        foreignKeysConstraints: [],
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
                    commands: [],
                    errors: []
                }
            });
        });

    });

});
