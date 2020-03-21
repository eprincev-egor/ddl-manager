import PgParser from "../../../lib/parser/pg/PgParser";
import assert from "assert";
import ExtensionModel from "../../../lib/objects/ExtensionModel";
import {
    Extension
} from "grapeql-lang";

describe("PgParser", () => {

    it("parseFile with simple extension", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            extension companies_note for companies (
                note text
            )
        `);

        assert.ok(result.length === 1, "result.length === 1");
        assert.ok(
            result[0] instanceof ExtensionModel, 
            "instanceof ExtensionModel"
        );
        assert.ok(
            result[0].get("parsed") instanceof Extension, 
            "parsed instanceof Extension"
        );
        
        const expectedSyntaxJSON: Extension["TJson"] = {
            forTable: {
                star: false,
                link: [
                    {
                        content: null,
                        word: "companies"
                    }
                ]
            },
            name: {
                word: "companies_note",
                content: null
            },
            deprecatedColumns: [],
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
                        word: "note"
                    },
                    type: {
                        type: "text"
                    },
                    unique: null,
                    nulls: true,
                    primaryKey: null
                }
            ]
        };

        const expectedModelJSON: ExtensionModel["TJson"] = {
            filePath: "test.sql",
            identify: "extension companies_note for public.companies",
            forTableIdentify: "public.companies",
            name: "companies_note",
            columns: [
                {
                    filePath: "test.sql",
                    identify: "note",
                    key: "note",
                    type: "text",
                    nulls: true,
                    parsed: {
                        check: null,
                        default: null,
                        foreignKey: null,
                        name: {
                            content: null,
                            word: "note"
                        },
                        type: {
                            type: "text"
                        },
                        unique: null,
                        nulls: true,
                        primaryKey: null
                    }
                }
            ],
            deprecated: false,
            deprecatedColumns: [],
            primaryKey: null,
            checkConstraints: [],
            foreignKeysConstraints: [],
            uniqueConstraints: [],
            rows: null,
            parsed: expectedSyntaxJSON
        };

        assert.deepStrictEqual(result[0].toJSON(), expectedModelJSON);
    });
    
    
    it("parse deprecated extension", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            deprecated 
            extension companies_note 
            for companies (
                note text
            )
        `);

        const extensionModel = result[0] as ExtensionModel;

        assert.strictEqual(extensionModel.get("deprecated"), true);
    });

   
    it("parse extension with deprecated columns", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            extension companies_note
            for companies (
                note text
            )
            deprecated (
                note
            )
        `);

        const extensionModel = result[0] as ExtensionModel;

        assert.deepStrictEqual(extensionModel.get("deprecatedColumns"), ["note"]);
    });
 
});