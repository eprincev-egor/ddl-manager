import {testGenerateMigration} from "./testGenerateMigration";

describe("Migration: triggers", () => {

    describe("create/drop triggers", () => {
        
        it("create trigger", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger"
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Trigger",
                            trigger: {
                                filePath: null,
                                identify: "create_role_trigger on public.company",
                                tableIdentify: "public.company",
                                functionIdentify: "public.create_role()",
                                name: "create_role_trigger",
                                parsed: null,
                                createdByDDLManager: true
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("drop trigger", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "Trigger",
                            trigger: {
                                filePath: null,
                                identify: "create_role_trigger on public.company",
                                tableIdentify: "public.company",
                                functionIdentify: "public.create_role()",
                                name: "create_role_trigger",
                                parsed: null,
                                createdByDDLManager: true
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("error on unknown table", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        filePath: "my_trigger.sql",
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger"
                    }]
                },
                db: {
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "my_trigger.sql",
                            code: "UnknownTableForTriggerError",
                            message: "not found table public.company for trigger create_role_trigger",
                            tableIdentify: "public.company",
                            triggerName: "create_role_trigger"
                        }
                    ]
                }
            });
        });

        
        it("error on unknown function", () => {
            testGenerateMigration({
                fs: {
                    triggers: [{
                        filePath: "my_trigger.sql",
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger"
                    }]
                },
                db: {
                    
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "my_trigger.sql",
                            code: "UnknownFunctionForTriggerError",
                            message: "not found function public.create_role() for trigger create_role_trigger",
                            functionIdentify: "public.create_role()",
                            triggerName: "create_role_trigger"
                        }
                    ]
                }
            });
        });
        
        it("maximum trigger name size is 64 symbols", () => {
            testGenerateMigration({
                fs: {
                    triggers: [{
                        filePath: "my_trigger.sql",
                        identify: "public.abcd012345678901234567890123456789012345678901234567890123456789_tail on public.company",
                        name: "abcd012345678901234567890123456789012345678901234567890123456789_tail",
                        functionIdentify: "public.test()",
                        tableIdentify: "public.company"
                    }]
                },
                db: {
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "my_trigger.sql",
                            code: "MaxObjectNameSizeError",
                            message: "trigger name too long: abcd012345678901234567890123456789012345678901234567890123456789_tail, max size is 64 symbols",
                            name: "abcd012345678901234567890123456789012345678901234567890123456789_tail",
                            objectType: "trigger"
                        }
                    ]
                }
            });
        });

        it("fs and db has same trigger, empty migration", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger"
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger"
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });
        
        
        it("change trigger (drop/create)", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger",
                        parsed: "xxx"
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger",
                        parsed: "yyy"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "Trigger",
                            trigger: {
                                filePath: null,
                                identify: "create_role_trigger on public.company",
                                tableIdentify: "public.company",
                                functionIdentify: "public.create_role()",
                                name: "create_role_trigger",
                                parsed: "yyy",
                                createdByDDLManager: true
                            }
                        },
                        {
                            type: "create",
                            command: "Trigger",
                            trigger: {
                                filePath: null,
                                identify: "create_role_trigger on public.company",
                                tableIdentify: "public.company",
                                functionIdentify: "public.create_role()",
                                name: "create_role_trigger",
                                parsed: "xxx",
                                createdByDDLManager: true
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("don't drop trigger if he was created without ddl-manager", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger",
                        parsed: "yyy",
                        createdByDDLManager: false
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

        it("replace trigger if he was created without ddl-manager and" +
        " exists trigger with same identify", () => {

            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger",
                        parsed: "xxx"
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            default: null,
                            type: "integer"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger",
                        parsed: "yyy",
                        createdByDDLManager: false
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "Trigger",
                            trigger: {
                                filePath: null,
                                identify: "create_role_trigger on public.company",
                                tableIdentify: "public.company",
                                functionIdentify: "public.create_role()",
                                name: "create_role_trigger",
                                parsed: "yyy",
                                createdByDDLManager: false
                            }
                        },
                        {
                            type: "create",
                            command: "Trigger",
                            trigger: {
                                filePath: null,
                                identify: "create_role_trigger on public.company",
                                tableIdentify: "public.company",
                                functionIdentify: "public.create_role()",
                                name: "create_role_trigger",
                                parsed: "xxx",
                                createdByDDLManager: true
                            }
                        }
                    ],
                    errors: []
                }
            });
    
        });
        
    });
    
});
