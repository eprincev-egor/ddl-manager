import {testGenerateMigration} from "../testGenerateMigration";
import { table, column, columnID } from "../fixtures/tables";

describe("Migration: tables", () => {
    const tables = {
        company: table("company", 
            columnID,
            column("id_country", "integer")
        ),
        country: table("country", 
            columnID,
            column("code", "text")
        )
    };

    describe("create foreign key", () => {
        
        it("create foreign key", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            ...tables.company,
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"]
                                }
                            ]
                        },
                        table("country", 
                            columnID,
                            column("code", "text")
                        )
                    ]
                },
                db: {
                    tables: [
                        tables.company,
                        tables.country
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "create",
                            command: "ForeignKeyConstraint",
                            tableIdentify: "public.company",
                            foreignKey: {
                                filePath: null,
                                parsed: null,
                                identify: "country_fk",
                                name: "country_fk",
                                columns: ["id_country"],
                                referenceTableIdentify: "public.country",
                                referenceColumns: ["id"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("drop foreign key", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        tables.company,
                        tables.country
                    ]
                },
                db: {
                    tables: [
                        {
                            ...tables.company,
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"]
                                }
                            ]
                        },
                        tables.country
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "ForeignKeyConstraint",
                            tableIdentify: "public.company",
                            foreignKey: {
                                filePath: null,
                                parsed: null,
                                identify: "country_fk",
                                name: "country_fk",
                                columns: ["id_country"],
                                referenceTableIdentify: "public.country",
                                referenceColumns: ["id"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });

        
        it("change foreign key (drop/create)", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            ...tables.company,
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"],
                                    parsed: "xxx"
                                }
                            ]
                        },
                        tables.country
                    ]
                },
                db: {
                    tables: [
                        {
                            ...tables.company,
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"],
                                    parsed: "yyy"
                                }
                            ]
                        },
                        tables.country
                    ]
                },
                migration: {
                    commands: [
                        {
                            type: "drop",
                            command: "ForeignKeyConstraint",
                            tableIdentify: "public.company",
                            foreignKey: {
                                filePath: null,
                                parsed: "yyy",
                                identify: "country_fk",
                                name: "country_fk",
                                columns: ["id_country"],
                                referenceTableIdentify: "public.country",
                                referenceColumns: ["id"]
                            }
                        },
                        {
                            type: "create",
                            command: "ForeignKeyConstraint",
                            tableIdentify: "public.company",
                            foreignKey: {
                                filePath: null,
                                parsed: "xxx",
                                identify: "country_fk",
                                name: "country_fk",
                                columns: ["id_country"],
                                referenceTableIdentify: "public.country",
                                referenceColumns: ["id"]
                            }
                        }
                    ],
                    errors: []
                }
            });
        });


        it("same foreign key, nothing to do", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            ...tables.company,
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"]
                                }
                            ]
                        },
                        tables.country
                    ]
                },
                db: {
                    tables: [
                        {
                            ...tables.company,
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id"]
                                }
                            ]
                        },
                        tables.country
                    ]
                },
                migration: {
                    commands: [],
                    errors: []
                }
            });
        });

        it("foreign key reference to unknown table", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            ...tables.company,
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country2",
                                    referenceColumns: ["id"]
                                }
                            ]
                        },
                        tables.country
                    ]
                },
                db: {
                    tables: [
                        tables.company,
                        tables.country
                    ]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            code: "ReferenceToUnknownTableError",
                            filePath: "company.sql",
                            foreignKeyName: "country_fk",
                            message: "foreign key 'country_fk' on table 'public.company' reference to unknown table 'public.country2'",
                            referenceTableIdentify: "public.country2",
                            tableIdentify: "public.company"
                        }
                    ]
                }
            });
        });


        it("foreign key reference to unknown column", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            ...tables.company,
                            foreignKeysConstraints: [
                                {
                                    identify: "country_fk",
                                    name: "country_fk",
                                    columns: ["id_country"],
                                    referenceTableIdentify: "public.country",
                                    referenceColumns: ["id2"]
                                }
                            ]
                        },
                        tables.country
                    ]
                },
                db: {
                    tables: [
                        tables.company,
                        tables.country
                    ]
                },
                migration: {
                    commands: [],
                    errors: [
                        {
                            code: "ReferenceToUnknownColumnError",
                            filePath: "company.sql",
                            foreignKeyName: "country_fk",
                            message: "foreign key 'country_fk' on table 'public.company' reference to unknown columns 'id2' in table 'public.country'",
                            referenceTableIdentify: "public.country",
                            referenceColumns: ["id2"],
                            tableIdentify: "public.company"
                        }
                    ]
                }
            });
        });

    });

});
