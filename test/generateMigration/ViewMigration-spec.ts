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

    });
    
});
