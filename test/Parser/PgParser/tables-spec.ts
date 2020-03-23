import {PgParser} from "../../../lib/parser/pg/PgParser";
import assert from "assert";
import {TableModel} from "../../../lib/objects/TableModel";
import {
    CreateTable
} from "grapeql-lang";

describe("PgParser", () => {

    it("parseFile with simple table", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            table companies (
                id serial primary key,
                name text not null
            )
        `);

        assert.ok(result.length === 1, "result.length === 1");
        assert.ok(
            result[0] instanceof TableModel, 
            "instanceof TableModel"
        );
        assert.ok(
            result[0].get("parsed") instanceof CreateTable, 
            "parsed instanceof CreateTable"
        );
        
        assert.deepStrictEqual(result[0].toJSON(), {
            filePath: "test.sql",
            identify: "public.companies",
            name: "companies",
            columns: [
                {
                    filePath: "test.sql",
                    identify: "id",
                    key: "id",
                    type: "serial",
                    nulls: false,
                    parsed: {
                        check: null,
                        default: null,
                        foreignKey: null,
                        name: {
                            content: null,
                            word: "id"
                        },
                        type: {
                            type: "serial"
                        },
                        unique: null,
                        nulls: false,
                        primaryKey: {
                            name: null,
                            column: {
                                content: null,
                                word: "id"
                            },
                            primaryKey: [
                                {
                                    content: null,
                                    word: "id"
                                }
                            ]
                        }
                    }
                },
                {
                    filePath: "test.sql",
                    identify: "name",
                    key: "name",
                    type: "text",
                    nulls: false,
                    parsed: {
                        check: null,
                        default: null,
                        foreignKey: null,
                        name: {
                            content: null,
                            word: "name"
                        },
                        type: {
                            type: "text"
                        },
                        unique: null,
                        nulls: false,
                        primaryKey: null
                    }
                }
            ],
            deprecated: false,
            deprecatedColumns: [],
            primaryKey: ["id"],
            checkConstraints: [],
            foreignKeysConstraints: [],
            uniqueConstraints: [],
            values: null,
            parsed: {
                schema: null,
                name: {
                    content: null,
                    word: "companies"
                },
                deprecatedColumns: [],
                inherits: [],
                constraints: [],
                deprecated: false,
                valuesRows: [],
                columns: [
                    {
                        check: null,
                        default: null,
                        foreignKey: null,
                        name: {
                            content: null,
                            word: "id"
                        },
                        type: {
                            type: "serial"
                        },
                        unique: null,
                        nulls: false,
                        primaryKey: {
                            name: null,
                            column: {
                                content: null,
                                word: "id"
                            },
                            primaryKey: [
                                {
                                    content: null,
                                    word: "id"
                                }
                            ]
                        }
                    },
                    {
                        check: null,
                        default: null,
                        foreignKey: null,
                        name: {
                            content: null,
                            word: "name"
                        },
                        type: {
                            type: "text"
                        },
                        unique: null,
                        nulls: false,
                        primaryKey: null
                    }
                ]
            }
        });
    });
    
    
    it("parse deprecated table", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            deprecated table companies (
                id serial primary key,
                name text not null
            )
        `);

        const tableModel = result[0] as TableModel;

        assert.strictEqual(tableModel.get("deprecated"), true);
    });

    
    it("parse table with deprecated columns", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            create table companies (
                id serial primary key,
                name text not null
            )
            deprecated (
                note
            )
        `);

        const tableModel = result[0] as TableModel;

        assert.deepStrictEqual(tableModel.get("deprecatedColumns"), ["note"]);
    });

    it("parse table with values", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            create table order_type (
                id serial primary key,
                name text not null
            )
            values (
                (1, 'FCL'),
                (2, 'LRL')
            )
        `);

        const tableModel = result[0] as TableModel;

        assert.deepStrictEqual(tableModel.get("values"), [
            ["1", "'FCL'"],
            ["2", "'LRL'"]
        ]);
    });

    it("parse table with unique constraint inside column", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            create table order_type (
                id serial primary key,
                name text not null unique
            )
        `);

        const tableModel = result[0] as TableModel;

        assert.deepStrictEqual(tableModel.toJSON().uniqueConstraints, [
            {
                filePath: "test.sql",
                identify: "order_type_name_key", 
                name: "order_type_name_key", 
                unique: ["name"],
                parsed: {
                    name: null,
                    column: {content: null, word: "name"},
                    unique: [
                        {content: null, word: "name"}
                    ]
                }
            }
        ]);
    });

    it("parse table with unique constraint inside table body", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            create table order_type (
                id serial primary key,
                name text not null,
                constraint test_uniq unique (name)
            )
        `);

        const tableModel = result[0] as TableModel;

        assert.deepStrictEqual(tableModel.toJSON().uniqueConstraints, [
            {
                filePath: "test.sql",
                identify: "test_uniq", 
                name: "test_uniq", 
                unique: ["name"],
                parsed: {
                    name: {content: null, word: "test_uniq"},
                    column: null,
                    unique: [
                        {content: null, word: "name"}
                    ]
                }
            }
        ]);
    });

    it("parse table with check constraint at near column definition", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            create table order_type (
                id serial primary key,
                name text check( name is not null )
            )
        `);

        const tableModel = result[0] as TableModel;

        assert.deepStrictEqual(tableModel.toJSON().checkConstraints, [
            {
                filePath: "test.sql",
                identify: "order_type_name_check", 
                name: "order_type_name_check",
                parsed: {
                    name: null,
                    column: {
                        content: null,
                        word: "name"
                    },
                    check: {
                        elements: [
                            {link: [{
                                content: null, word: "name"
                            }], star: false},

                            {operator: "is not"},

                            {null: true}
                        ]
                    }
                }
            }
        ]);
    });

    it("parse table with check constraint inside table body", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            create table order_type (
                id serial primary key,
                name text, 
                constraint hello_name check( name is not null )
            )
        `);

        const tableModel = result[0] as TableModel;

        assert.deepStrictEqual(tableModel.toJSON().checkConstraints, [
            {
                filePath: "test.sql",
                identify: "hello_name", 
                name: "hello_name",
                parsed: {
                    name: {
                        content: null,
                        word: "hello_name"
                    },
                    column: null,
                    check: {
                        elements: [
                            {link: [{
                                content: null, word: "name"
                            }], star: false},

                            {operator: "is not"},
                            
                            {null: true}
                        ]
                    }
                }
            }
        ]);
    });

    it("parse table with foreign key at near column definition", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            create table companies (
                id serial primary key,
                country_id integer references countries
            )
        `);

        const tableModel = result[0] as TableModel;

        assert.deepStrictEqual(tableModel.toJSON().foreignKeysConstraints, [
            {
                filePath: "test.sql",
                identify: "companies_country_id_fkey", 
                name: "companies_country_id_fkey",
                columns: ["country_id"],
                referenceColumns: ["id"],
                referenceTableIdentify: "countries",
                parsed: {
                    name: null,
                    column: {
                        content: null,
                        word: "country_id"
                    },
                    columns: [{
                        content: null,
                        word: "country_id"
                    }],
                    match: null,
                    onDelete: null,
                    onUpdate: null,
                    referenceTable: {link: [
                        {content: null, word: "countries"}
                    ], star: false},
                    referenceColumns: null
                }
            }
        ]);
    });

    it("parse table with foreign key inside table body", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            create table companies (
                id serial primary key,
                country_id integer,
                constraint country_fk 
                    foreign key (country_id) 
                    references countries
            )
        `);

        const tableModel = result[0] as TableModel;

        assert.deepStrictEqual(tableModel.toJSON().foreignKeysConstraints, [
            {
                filePath: "test.sql",
                identify: "country_fk", 
                name: "country_fk",
                columns: ["country_id"],
                referenceColumns: ["id"],
                referenceTableIdentify: "countries",
                parsed: {
                    name: {
                        content: null,
                        word: "country_fk"
                    },
                    column: null,
                    columns: [{
                        content: null,
                        word: "country_id"
                    }],
                    match: null,
                    onDelete: null,
                    onUpdate: null,
                    referenceTable: {link: [
                        {content: null, word: "countries"}
                    ], star: false},
                    referenceColumns: null
                }
            }
        ]);
    });

});