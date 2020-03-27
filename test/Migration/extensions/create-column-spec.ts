import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: extensions", () => {

    describe("create column", () => {
        
        it("create column for existent table by extension", () => {
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
                        }]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Column",
                            tableIdentify: "public.companies",
                            column: {
                                filePath: null,
                                identify: "name",
                                key: "name",
                                parsed: null,
                                nulls: true,
                                type: "text"
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        it("create two columns from two extensions", () => {
            testGenerateMigration({
                fs: {
                    extensions: [
                        {
                            filePath: "name_for_companies.sql",
                            name: "test",
                            identify: "extension name for public.companies",
                            forTableIdentify: "public.companies",
                            columns: [{
                                identify: "name",
                                key: "name",
                                type: "text"
                            }]
                        },
                        {
                            filePath: "inn_for_companies.sql",
                            name: "test",
                            identify: "extension inn for public.companies",
                            forTableIdentify: "public.companies",
                            columns: [{
                                identify: "inn",
                                key: "inn",
                                type: "text"
                            }]
                        }
                    ],
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
                        }]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Column",
                            tableIdentify: "public.companies",
                            column: {
                                filePath: null,
                                identify: "name",
                                key: "name",
                                parsed: null,
                                nulls: true,
                                type: "text"
                            }
                        },
                        {
                            type: "create",
                            command: "Column",
                            tableIdentify: "public.companies",
                            column: {
                                filePath: null,
                                identify: "inn",
                                key: "inn",
                                parsed: null,
                                nulls: true,
                                type: "text"
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

    });

});
