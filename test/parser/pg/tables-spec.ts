import PgParser from "../../../lib/parser/PgParser";
import assert from "assert";
import TableModel from "../../../lib/objects/TableModel";
import {
    CreateTable
} from "grapeql-lang";

describe("PgParser", () => {

    it("parseFile with simple table", async() => {
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
    
});