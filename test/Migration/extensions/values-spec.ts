import {testGenerateMigration} from "../testGenerateMigration";

describe("MigrationController", () => {

    describe("generateExtensions", () => {
        
        it("create values from extension", () => {
            testGenerateMigration({
                options: {
                    mode: "dev"
                },
                fs: {
                    extensions: [{
                        filePath: "values_for_order_type.sql",
                        name: "values",
                        identify: "extension values for public.order_type",
                        forTableIdentify: "public.order_type",
                        columns: [],
                        values: [
                            ["1", "FCL"]
                        ]
                    }],
                    tables: [{
                        filePath: "order_type.sql",
                        identify: "public.order_type",
                        name: "order_type",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                identify: "name",
                                key: "name",
                                type: "text"
                            }
                        ],
                        primaryKey: ["id"]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.order_type",
                        name: "order_type",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                identify: "name",
                                key: "name",
                                type: "text"
                            }
                        ],
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
                        filePath: "values_for_order_type.sql",
                        name: "values",
                        identify: "extension values for public.order_type",
                        forTableIdentify: "public.order_type",
                        columns: [],
                        values: [
                            ["1", "LTL"]
                        ]
                    }],
                    tables: [{
                        filePath: "order_type.sql",
                        identify: "public.order_type",
                        name: "order_type",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                identify: "name",
                                key: "name",
                                type: "text"
                            }
                        ],
                        primaryKey: ["id"],
                        values: [
                            ["1", "FCL"]
                        ]
                    }]
                },
                db: {
                    tables: [{
                        identify: "public.order_type",
                        name: "order_type",
                        columns: [
                            {
                                identify: "id",
                                key: "id",
                                type: "integer"
                            },
                            {
                                identify: "name",
                                key: "name",
                                type: "text"
                            }
                        ],
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
