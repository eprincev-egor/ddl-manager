import PgParser from "../../../lib/parser/PgParser";
import assert from "assert";
import ViewModel from "../../../lib/objects/ViewModel";
import {
    CreateView
} from "grapeql-lang";

describe("PgParser", () => {

    it("parseFile with simple view", async() => {
        const parser = new PgParser();
        
        const result = parser.parseFile("test.sql", `
            create view hello_view as
                select 1
        `);

        assert.ok(result.length === 1, "result.length === 1");
        assert.ok(
            result[0] instanceof ViewModel, 
            "instanceof ViewModel"
        );
        assert.ok(
            result[0].get("parsed") instanceof CreateView, 
            "parsed instanceof CreateView"
        );
        
        assert.deepStrictEqual(result[0].toJSON(), {
            filePath: "test.sql",
            identify: "public.hello_view",
            name: "hello_view",
            createdByDDLManager: true,
            parsed: {
                name: {
                    content: null,
                    word: "hello_view"
                },
                schema: null,
                select: {
                    columns: [
                        {
                            as: null,
                            expression: {
                                elements: [
                                    {number: "1"}
                                ]
                            }
                        }
                    ],
                    fetch: null,
                    from: null,
                    groupBy: null,
                    having: null,
                    limit: null,
                    offset: null,
                    offsetRow: null,
                    offsetRows: null,
                    orderBy: null,
                    union: null,
                    where: null,
                    window: null,
                    with: null
                }
            }
        });
    });
    
});