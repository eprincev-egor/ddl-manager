import {testGenerateMigration} from "../testGenerateMigration";
import { extension, table, columnNAME, columnID } from "../fixtures/tables";

describe("Migration: extensions", () => {

    describe("values (rows)", () => {
        
        it("create values from extension", () => {
            testGenerateMigration({
                options: {
                    mode: "dev"
                },
                fs: {
                    extensions: [{
                        ...extension("values", "order_type"),
                        columns: [],
                        values: [
                            ["1", "FCL"]
                        ]
                    }],
                    tables: [{
                        ...table("order_type", columnID, columnNAME),
                        primaryKey: ["id"]
                    }]
                },
                db: {
                    tables: [{
                        ...table("order_type", columnID, columnNAME),
                        primaryKey: ["id"]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Rows",
                            table: {
                                filePath: "order_type.sql",
                                identify: "public.order_type",
                                name: "order_type",
                                parsed: null,
                                deprecatedColumns: [],
                                deprecated: false,
                                uniqueConstraints: [],
                                foreignKeysConstraints: [],
                                checkConstraints: [],
                                columns: [
                                    {
                                        filePath: null,
                                        identify: "id",
                                        key: "id",
                                        parsed: null,
                                        nulls: true,
                                        type: "integer"
                                    },
                                    {
                                        filePath: null,
                                        identify: "name",
                                        key: "name",
                                        parsed: null,
                                        nulls: true,
                                        type: "text"
                                    }
                                ],
                                primaryKey: ["id"],
                                values: [
                                    ["1", "FCL"]
                                ]
                            },
                            values: [
                                ["1", "FCL"]
                            ]
                        }
                    ],
                    errors: []
                }
            });
        });

        it("replace values from extension", () => {
            testGenerateMigration({
                options: {
                    mode: "dev"
                },
                fs: {
                    extensions: [{
                        ...extension("values", "order_type"),
                        columns: [],
                        values: [
                            ["1", "LTL"]
                        ]
                    }],
                    tables: [{
                        ...table("order_type", columnID, columnNAME),
                        primaryKey: ["id"],
                        values: [
                            ["1", "FCL"]
                        ]
                    }]
                },
                db: {
                    tables: [{
                        ...table("order_type", columnID, columnNAME),
                        primaryKey: ["id"]
                    }]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "Rows",
                            table: {
                                filePath: "order_type.sql",
                                identify: "public.order_type",
                                name: "order_type",
                                parsed: null,
                                deprecatedColumns: [],
                                deprecated: false,
                                uniqueConstraints: [],
                                foreignKeysConstraints: [],
                                checkConstraints: [],
                                columns: [
                                    {
                                        filePath: null,
                                        identify: "id",
                                        key: "id",
                                        parsed: null,
                                        nulls: true,
                                        type: "integer"
                                    },
                                    {
                                        filePath: null,
                                        identify: "name",
                                        key: "name",
                                        parsed: null,
                                        nulls: true,
                                        type: "text"
                                    }
                                ],
                                primaryKey: ["id"],
                                values: [
                                    ["1", "LTL"]
                                ]
                            },
                            values: [
                                ["1", "LTL"]
                            ]
                        }
                    ],
                    errors: []
                }
            });
        });

    });
    
});
