import {PgParser} from "../../../lib/parser/pg/PgParser";
import assert from "assert";
import {ExtensionModel} from "../../../lib/objects/ExtensionModel";
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
            values: null,
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
 
    it("parse extension with rows", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            extension companies_note
            for order_type
            values (
                (1, 'FCL'),
                (2, 'LRL')
            )
        `);

        const extensionModel = result[0] as ExtensionModel;

        assert.deepStrictEqual(extensionModel.get("values"), [
            ["1", "'FCL'"],
            ["2", "'LRL'"]
        ]);
    });

    it("parse extension with unique constraint inside column", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            extension test
            for order_type (
                name text not null unique
            )
        `);

        const extensionModel = result[0] as ExtensionModel;

        assert.deepStrictEqual(extensionModel.toJSON().uniqueConstraints, [
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
    
    it("parse extension with unique constraint inside body", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            extension test
            for order_type (
                constraint test_uniq unique (name)
            )
        `);

        const extensionModel = result[0] as ExtensionModel;

        assert.deepStrictEqual(extensionModel.toJSON().uniqueConstraints, [
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

    it("parse extension with check constraint at near column definition", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            extension test
            for order_type (
                name text check( name is not null )
            )
        `);

        const extensionModel = result[0] as ExtensionModel;

        assert.deepStrictEqual(extensionModel.toJSON().checkConstraints, [
            {
                filePath: "test.sql",
                identify: "order_type_name_check", 
                name: "order_type_name_check",
                check: "name is not null",
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

    it("parse table with check constraint inside body", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            extension test
            for order_type ( 
                constraint hello_name check( name is not null )
            )
        `);

        const extensionModel = result[0] as ExtensionModel;

        assert.deepStrictEqual(extensionModel.toJSON().checkConstraints, [
            {
                filePath: "test.sql",
                identify: "hello_name", 
                name: "hello_name",
                check: "name is not null",
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

    it("parse extension with foreign key at near column definition", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            extension test
            for companies (
                country_id integer references countries
            )
        `);

        const extensionModel = result[0] as ExtensionModel;

        assert.deepStrictEqual(extensionModel.toJSON().foreignKeysConstraints, [
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

    it("parse extension with foreign key inside body", () => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            extension test
            for companies (
                constraint country_fk 
                    foreign key (country_id) 
                    references countries
            )
        `);

        const extensionModel = result[0] as ExtensionModel;

        assert.deepStrictEqual(extensionModel.toJSON().foreignKeysConstraints, [
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