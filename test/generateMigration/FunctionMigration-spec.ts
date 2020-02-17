import testGenerateMigration from "./testGenerateMigration";

describe("State", () => {

    describe("generateMigration for functions", () => {
        
        it("create function for empty db", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        identify: "public.test()",
                        name: "test"
                    }]
                },
                db: {
                    functions: []
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            function: {
                                identify: "public.test()",
                                name: "test",
                                parsed: null
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("remove function for db with one function", () => {
            testGenerateMigration({
                fs: {
                    functions: []
                },
                db: {
                    functions: [{
                        identify: "public.test()",
                        name: "test"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            function: {
                                identify: "public.test()",
                                name: "test",
                                parsed: null
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("db and fs has only one function, empty migration", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        identify: "public.test()",
                        name: "test"
                    }]
                },
                db: {
                    functions: [{
                        identify: "public.test()",
                        name: "test"
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

        it("db and fs has only one function, but that different functions", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        identify: "public.test1()",
                        name: "test1"
                    }]
                },
                db: {
                    functions: [{
                        identify: "public.test2()",
                        name: "test2"
                    }]
                },
                migration: {
                    commands: [
                        // first the 'drop'
                        {
                            type: "drop",
                            function: {
                                identify: "public.test2()",
                                name: "test2",
                                parsed: null
                            }
                        },
                        // second the 'create'
                        {
                            type: "create",
                            function: {
                                identify: "public.test1()",
                                name: "test1",
                                parsed: null
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

    });
    
});
