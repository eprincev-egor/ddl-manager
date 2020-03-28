import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: extensions", () => {

    describe("unique constraints", () => {
        
        it("create unique constraint from extension", () => {
            testGenerateMigration({
                fs: {
                    extensions: [{
                        filePath: "test_for_companies.sql",
                        name: "test",
                        identify: "extension test for public.companies",
                        forTableIdentify: "public.companies",
                        columns: [{
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }],
                    tables: [{
                        filePath: "companies.sql",
                        identify: "public.companies",
                        name: "companies",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.companies",
                        name: "companies",
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
                            command: "UniqueConstraint",
                            tableIdentify: "public.companies",
                            unique: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                unique: ["name"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("drop unique constraint from extension", () => {
            testGenerateMigration({
                fs: {
                    extensions: [{
                        filePath: "test_for_companies.sql",
                        name: "test",
                        identify: "extension test for public.companies",
                        forTableIdentify: "public.companies",
                        columns: [{
                            identify: "name",
                            key: "name",
                            type: "text"
                        }]
                    }],
                    tables: [{
                        filePath: "companies.sql",
                        identify: "public.companies",
                        name: "companies",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.companies",
                        name: "companies",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "UniqueConstraint",
                            tableIdentify: "public.companies",
                            unique: {
                                identify: "name",
                                filePath: null,
                                name: "name",
                                parsed: null,
                                unique: ["name"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });
        
    });

});
