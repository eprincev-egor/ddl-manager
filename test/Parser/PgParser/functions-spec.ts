import {PgParser} from "../../../lib/parser/pg/PgParser";
import assert from "assert";
import {FunctionModel} from "../../../lib/objects/FunctionModel";
import {
    CreateFunction
} from "grapeql-lang";

describe("PgParser", () => {

    it("parseFile: simple function", async() => {
        const parser = new PgParser();

        const body = `
        begin
        end
        `;
        
        const result = parser.parseFile("test.sql", `
            create function public.test()
            returns void as $body$${body}$body$
            language plpgsql;
        `);

        assert.ok(result.length === 1, "result.length === 1");
        assert.ok(
            result[0] instanceof FunctionModel, 
            "instanceof FunctionModel"
        );
        assert.ok(
            result[0].get("parsed") instanceof CreateFunction, 
            "parsed instanceof CreateFunction"
        );
        
        assert.deepStrictEqual(result[0].toJSON(), {
            filePath: "test.sql",
            identify: "public.test()",
            name: "test",
            createdByDDLManager: true,
            parsed: {
                args: [],
                body: {
                    content: body
                },
                comment: null,
                immutable: null,
                language: "plpgsql",
                name: "test",
                parallel: null,
                returns: {
                    setof: null,
                    table: null,
                    type: "void"
                },
                cost: null,
                returnsNullOnNull: null,
                schema: "public",
                stable: null,
                strict: null
            }
        });
    });
    
    it("parseFile: function with arguments", async() => {
        const parser = new PgParser();

        const body = `
        begin
        end
        `;
        
        const result = parser.parseFile("test.sql", `
            create function public.test(x integer, y bigint default 1)
            returns void as $body$${body}$body$
            language plpgsql;
        `);

        assert.ok(result.length === 1, "result.length === 1");
        assert.ok(
            result[0] instanceof FunctionModel, 
            "instanceof FunctionModel"
        );
        assert.ok(
            result[0].get("parsed") instanceof CreateFunction, 
            "parsed instanceof CreateFunction"
        );
        
        assert.deepStrictEqual(result[0].toJSON(), {
            filePath: "test.sql",
            identify: "public.test(integer,bigint)",
            name: "test",
            createdByDDLManager: true,
            parsed: {
                args: [
                    {
                        default: null,
                        in: null,
                        name: "x",
                        out: null,
                        type: "integer"
                    },
                    {
                        default: "1",
                        in: null,
                        name: "y",
                        out: null,
                        type: "bigint"
                    }
                ],
                body: {
                    content: body
                },
                comment: null,
                immutable: null,
                language: "plpgsql",
                name: "test",
                parallel: null,
                returns: {
                    setof: null,
                    table: null,
                    type: "void"
                },
                cost: null,
                returnsNullOnNull: null,
                schema: "public",
                stable: null,
                strict: null
            }
        });
    });
    
    it("parseFile: function with in/out arguments", async() => {
        const parser = new PgParser();

        const result = parser.parseFile("test.sql", `
            create function public.test(in x integer, out y bigint)
            returns void as $body$
                begin
                end
            $body$
            language plpgsql;
        `);

        assert.ok(result.length === 1, "result.length === 1");

        const func = result[0];
        assert.strictEqual(func.getIdentify(), "public.test(integer)");
    });
    
});