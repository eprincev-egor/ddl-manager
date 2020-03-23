import {PgParser} from "../../../lib/parser/pg/PgParser";
import assert from "assert";
import {FunctionModel} from "../../../lib/objects/FunctionModel";
import {TriggerModel} from "../../../lib/objects/TriggerModel";
import {
    CreateFunction,
    CreateTrigger
} from "grapeql-lang";

describe("PgParser", () => {

    it("parseFile with simple trigger", async() => {
        const parser = new PgParser();

        const body = `
        begin
        end
        `;
        
        const result = parser.parseFile("test.sql", `
            create function public.test()
            returns trigger as $body$${body}$body$
            language plpgsql;

            create trigger test_trigger
            after insert
            on companies
            for each row
            execute procedure test();
        `);

        assert.ok(result.length === 2, "result.length === 2");
        assert.ok(
            result[0] instanceof FunctionModel, 
            "instanceof FunctionModel"
        );
        assert.ok(
            result[0].get("parsed") instanceof CreateFunction, 
            "parsed instanceof CreateFunction"
        );

        assert.ok(
            result[1] instanceof TriggerModel, 
            "instanceof TriggerModel"
        );
        assert.ok(
            result[1].get("parsed") instanceof CreateTrigger, 
            "parsed instanceof CreateTrigger"
        );
        
        assert.deepStrictEqual(result[1].toJSON(), {
            filePath: "test.sql",
            identify: "test_trigger on public.companies",
            name: "test_trigger",
            createdByDDLManager: true,
            tableIdentify: "public.companies",
            functionIdentify: "public.test()",
            parsed: {
                name: "test_trigger",
            
                before: null,
                after: true,
                insert: true,
                delete: null,
                update: null,
                updateOf: null,
                
                table: {
                    name: "companies",
                    schema: "public"
                },

                constraint: null,
                deferrable: null,
                statement: null,
                initially: null,

                when: null,

                procedure: {
                    args: [],
                    name: "test",
                    schema: "public"
                },
                comment: null
            }
        });
    });
    
});