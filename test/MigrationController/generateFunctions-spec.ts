import {testGenerateMigration} from "./testGenerateMigration";

describe("MigrationController", () => {

    describe("generateFunctions", () => {
        
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
                            command: "Function",
                            function: {
                                filePath: null,
                                identify: "public.test()",
                                name: "test",
                                parsed: null,
                                createdByDDLManager: true
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
                            command: "Function",
                            function: {
                                filePath: null,
                                identify: "public.test()",
                                name: "test",
                                parsed: null,
                                createdByDDLManager: true
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
                            command: "Function",
                            function: {
                                filePath: null,
                                identify: "public.test2()",
                                name: "test2",
                                parsed: null,
                                createdByDDLManager: true
                            }
                        },
                        // second the 'create'
                        {
                            type: "create",
                            command: "Function",
                            function: {
                                filePath: null,
                                identify: "public.test1()",
                                name: "test1",
                                parsed: null,
                                createdByDDLManager: true
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("maximum function name size is 64 symbols", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        filePath: "my_func.sql",
                        identify: "public.abcd012345678901234567890123456789012345678901234567890123456789_tail()",
                        name: "abcd012345678901234567890123456789012345678901234567890123456789_tail"
                    }]
                },
                db: {
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "my_func.sql",
                            code: "MaxObjectNameSizeError",
                            message: "function name too long: abcd012345678901234567890123456789012345678901234567890123456789_tail, max size is 64 symbols",
                            name: "abcd012345678901234567890123456789012345678901234567890123456789_tail",
                            objectType: "function"
                        }
                    ]
                }
            });
        });

        it("don't drop function if she was created without ddl-manager", () => {
            testGenerateMigration({
                fs: {},
                db: {
                    functions: [{
                        identify: "public.sys_admin_func()",
                        name: "sys_admin_func",
                        createdByDDLManager: false
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

        it("change function (drop/create)", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        identify: "public.my_func()",
                        name: "my_func",
                        parsed: "xxx"
                    }]
                },
                db: {
                    functions: [{
                        identify: "public.my_func()",
                        name: "my_func",
                        parsed: "yyy"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "Function",
                            function: {
                                filePath: null,
                                identify: "public.my_func()",
                                name: "my_func",
                                parsed: "yyy",
                                createdByDDLManager: true
                            }
                        },
                        {
                            type: "create",
                            command: "Function",
                            function: {
                                filePath: null,
                                identify: "public.my_func()",
                                name: "my_func",
                                parsed: "xxx",
                                createdByDDLManager: true
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("replace function if she was created without ddl-manager and" +
        " exists function with same identify", () => {
            testGenerateMigration({
                fs: {
                    functions: [{
                        identify: "public.sys_admin_func()",
                        name: "sys_admin_func",
                        parsed: "xxx"
                    }]
                },
                db: {
                    functions: [{
                        identify: "public.sys_admin_func()",
                        name: "sys_admin_func",
                        createdByDDLManager: false,
                        parsed: "yyy"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "Function",
                            function: {
                                filePath: null,
                                identify: "public.sys_admin_func()",
                                name: "sys_admin_func",
                                parsed: "yyy",
                                createdByDDLManager: false
                            }
                        },
                        {
                            type: "create",
                            command: "Function",
                            function: {
                                filePath: null,
                                identify: "public.sys_admin_func()",
                                name: "sys_admin_func",
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
