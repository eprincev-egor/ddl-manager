import PgParser from "../../lib/parser/PgParser";
import assert from "assert";
import FunctionModel from "../../lib/objects/FunctionModel";

describe("PgParser", () => {

    it("parseFile with simple function", async() => {
        const parser = new PgParser();
        
        const result = parser.parseFile(`
            create function public.test()
            returns void as $body$
            begin
            end
            $body$
            language plpgsql;
        `);

        assert.ok(result.length === 1, "result.length === 1");
        assert.ok(result[0] instanceof FunctionModel, "instanceof FunctionModel");
        
        assert.deepStrictEqual(result[0].toJSON(), {
            schema: "public",
            name: "test",
            args: ""
        });
    });
    
});