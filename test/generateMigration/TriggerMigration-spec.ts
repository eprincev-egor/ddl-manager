import testGenerateMigration from "./testGenerateMigration";

describe("State", () => {

    describe("generateMigration for triggers", () => {
        
        it("create trigger", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        name: "create_role_trigger"
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id"
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
                            trigger: {
                                identify: "create_role_trigger on public.company",
                                tableIdentify: "public.company",
                                name: "create_role_trigger",
                                parsed: null
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
                            key: "id"
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
                            key: "id"
                        }]
                    }],
                    functions: [{
                        identify: "public.create_role()",
                        name: "create_role"
                    }],
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        name: "create_role_trigger"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            trigger: {
                                identify: "create_role_trigger on public.company",
                                tableIdentify: "public.company",
                                name: "create_role_trigger",
                                parsed: null
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
                    triggers: [{
                        identify: "create_role_trigger on public.company",
                        tableIdentify: "public.company",
                        name: "create_role_trigger"
                    }]
                },
                db: {
                    
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            code: "UnknownTableForTriggerErrorModel",
                            message: "not found table public.company for trigger create_role_trigger",
                            tableIdentify: "public.company",
                            triggerName: "create_role_trigger"
                        }
                    ]
                }
            });
        });
    });
    
});
