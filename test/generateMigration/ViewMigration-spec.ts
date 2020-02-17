import testGenerateMigration from "./testGenerateMigration";

describe("State", () => {

    describe("generateMigration for views", () => {
        
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
                            view: {
                                filePath: null,
                                identify: "public.operations_view",
                                name: "operations_view",
                                parsed: null
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
                            view: {
                                filePath: null,
                                identify: "public.operations_view",
                                name: "operations_view",
                                parsed: null
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

    });
    
});
