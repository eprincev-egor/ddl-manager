import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: tables", () => {

    describe("primary key", () => {
        
        it("create primary key", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }],
                        primaryKey: ["id"]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
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
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id"]
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("drop primary key", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }],
                        primaryKey: ["id"]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id"]
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("change primary key (other columns)", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        primaryKey: ["id"]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        primaryKey: ["id", "name"]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id", "name"]
                        },
                        {
                            type: "create",
                            command: "PrimaryKey",
                            tableIdentify: "public.company",
                            primaryKey: ["id"]
                        }
                    ],
                    errors: []
                }
            });
        });
        
        it("same primary key, but another keys sort, nothing to do", () => {
            testGenerateMigration({
                fs: {
                    tables: [{
                        filePath: "company.sql",
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        primaryKey: ["name", "id"]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.company",
                        name: "company",
                        columns: [{
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }, {
                            identify: "name",
                            key: "name",
                            type: "text"
                        }],
                        primaryKey: ["id", "name"]
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
