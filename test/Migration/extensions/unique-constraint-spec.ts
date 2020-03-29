import {testGenerateMigration} from "../testGenerateMigration";
import { companiesWithId, companiesWithIdAndName, columnNAME } from "./fixtures/tables";

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
                        columns: [
                            columnNAME
                        ],
                        uniqueConstraints: [
                            {
                                identify: "name",
                                name: "name",
                                unique: ["name"]
                            }
                        ]
                    }],
                    tables: [
                        companiesWithId
                    ]
                },
                db: {
                    tables: [
                        companiesWithIdAndName
                    ]
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
                        columns: [
                            columnNAME
                        ]
                    }],
                    tables: [
                        companiesWithId
                    ]
                },
                db: {
                    tables: [{
                        ...companiesWithIdAndName,
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
