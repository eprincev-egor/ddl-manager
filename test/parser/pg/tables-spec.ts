import PgParser from "../../../lib/parser/PgParser";
import assert from "assert";
import TableModel from "../../../lib/objects/TableModel";
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
            rows: null,
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
                values: [],
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

    it("parse table with rows", () => {
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

        assert.deepStrictEqual(tableModel.get("rows"), [
            {id: 1, name: "FCL"},
            {id: 2, name: "LRL"}
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

});