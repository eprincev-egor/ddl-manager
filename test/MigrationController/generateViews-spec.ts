import {testGenerateMigration} from "./testGenerateMigration";

describe("MigrationController", () => {

    describe("generateViews", () => {
        
        it("create view for empty db", () => {
            testGenerateMigration({
                fs: {
                    views: [{
                        identify: "public.operations_view",
                        name: "operations_view"
                    }]
                },
                db: {
                    views: []
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "View",
                            view: {
                                filePath: null,
                                identify: "public.operations_view",
                                name: "operations_view",
                                parsed: null,
                                createdByDDLManager: true
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("remove view for db with one view", () => {
            testGenerateMigration({
                fs: {
                    views: []
                },
                db: {
                    views: [{
                        identify: "public.operations_view",
                        name: "operations_view"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "View",
                            view: {
                                filePath: null,
                                identify: "public.operations_view",
                                name: "operations_view",
                                parsed: null,
                                createdByDDLManager: true
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("maximum view name size is 64 symbols", () => {
            testGenerateMigration({
                fs: {
                    views: [{
                        filePath: "my_view.sql",
                        identify: "public.abcd012345678901234567890123456789012345678901234567890123456789_tail",
                        name: "abcd012345678901234567890123456789012345678901234567890123456789_tail"
                    }]
                },
                db: {
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            filePath: "my_view.sql",
                            code: "MaxObjectNameSizeError",
                            message: "view name too long: abcd012345678901234567890123456789012345678901234567890123456789_tail, max size is 64 symbols",
                            name: "abcd012345678901234567890123456789012345678901234567890123456789_tail",
                            objectType: "view"
                        }
                    ]
                }
            });
        });

        it("fs and db has same view, empty migration", () => {
            testGenerateMigration({
                fs: {
                    views: [{
                        identify: "public.operations_view",
                        name: "operations_view"
                    }]
                },
                db: {
                    views: [{
                        identify: "public.operations_view",
                        name: "operations_view"
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

        
        it("change view (drop/create)", () => {
            testGenerateMigration({
                fs: {
                    views: [{
                        identify: "public.operations_view",
                        name: "operations_view",
                        parsed: "xxx"
                    }]
                },
                db: {
                    views: [{
                        identify: "public.operations_view",
                        name: "operations_view",
                        parsed: "yyy"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "View",
                            view: {
                                filePath: null,
                                identify: "public.operations_view",
                                name: "operations_view",
                                parsed: "yyy",
                                createdByDDLManager: true
                            }
                        },
                        {
                            type: "create",
                            command: "View",
                            view: {
                                filePath: null,
                                identify: "public.operations_view",
                                name: "operations_view",
                                parsed: "xxx",
                                createdByDDLManager: true
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("don't drop view if she was created without ddl-manager", () => {
            testGenerateMigration({
                fs: {},
                db: {
                    views: [{
                        identify: "public.operations_view",
                        name: "operations_view",
                        createdByDDLManager: false
                    }]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });


        it("replace view if she was created without ddl-manager and" +
        " exists view with same identify", () => {
            testGenerateMigration({
                fs: {
                    views: [{
                        identify: "public.operations_view",
                        name: "operations_view",
                        parsed: "xxx"
                    }]
                },
                db: {
                    views: [{
                        identify: "public.operations_view",
                        name: "operations_view",
                        createdByDDLManager: false,
                        parsed: "yyy"
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "View",
                            view: {
                                filePath: null,
                                identify: "public.operations_view",
                                name: "operations_view",
                                parsed: "yyy",
                                createdByDDLManager: false
                            }
                        },
                        {
                            type: "create",
                            command: "View",
                            view: {
                                filePath: null,
                                identify: "public.operations_view",
                                name: "operations_view",
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
