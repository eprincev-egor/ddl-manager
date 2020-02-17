import testGenerateMigration from "./testGenerateMigration";

describe("State", () => {

    describe("generateMigration for tables", () => {
        
        it("create table for empty db", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id"
                        }]
                    }]
                },
                db: {
                    tables: []
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            table: {
                                identify: "public.company",
                                name: "company",
                                parsed: null,
                                columns: [{
                                    identify: "id",
                                    key: "id",
                                    parsed: null
                                }]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("create column", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id"
                        }, {
                            identify: "name",
                            key: "name"
                        }]
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
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            tableIdentify: "public.company",
                            column: {
                                identify: "name",
                                key: "name",
                                parsed: null
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("db and fs has only one same table, empty migration", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id"
                        }, {
                            identify: "name",
                            key: "name"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id"
                        }, {
                            identify: "name",
                            key: "name"
                        }]
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
