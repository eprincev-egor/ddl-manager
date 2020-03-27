import {testGenerateMigration} from "../testGenerateMigration";

describe("Migration: tables", () => {

    describe("create foreign key", () => {
        
        it("create foreign key", () => {
            testGenerateMigration({
                fs: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
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
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
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
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
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
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
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
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
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
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
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
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
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
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
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
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
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
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
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
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
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
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
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
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }],
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
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
                    ]
                },
                db: {
                    tables: [
                        {
                            filePath: "company.sql",
                            identify: "public.company",
                            name: "company",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "id_country",
                                key: "id_country",
                                type: "integer"
                            }]
                        },
                        {
                            filePath: "country.sql",
                            identify: "public.country",
                            name: "country",
                            columns: [{
                                identify: "id",
                                key: "id",
                                type: "integer"
                            }, {
                                identify: "code",
                                key: "code",
                                type: "text"
                            }]
                        }
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
