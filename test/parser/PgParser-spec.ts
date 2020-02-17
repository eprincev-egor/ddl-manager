import PgParser from "../../lib/parser/PgParser";
import assert from "assert";
import FunctionModel from "../../lib/objects/FunctionModel";
import {
    CreateFunction
} from "grapeql-lang";

describe("PgParser", () => {

    it("parseFile with simple function", async() => {
        const parser = new PgParser();

        const body = `
        begin
        end
        `;
        
        const result = parser.parseFile(`
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
            identify: "public.test()",
            name: "test",
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
    
});