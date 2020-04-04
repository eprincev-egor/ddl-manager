"use strict";

const body = `
    begin
        raise notice 'test 1';
    end
`;

module.exports = [
    {
        createdByDDLManager: false,
        filePath: "(database)",
        identify: "public.some_func(text)",
        name: "some_func",
        parsed: {
            schema: "public",
            name: "some_func",
            args: [
                {
                    name: null,
                    type: "text",
                    out: null,
                    in: null,
                    default: null
                }
            ],
            body: {
                content: body
            },
            comment: null,
            cost: null,
            language: "plpgsql",
            parallel: null,
            returns: {
                setof: null,
                table: null,
                type: "void"
            },
            immutable: null,
            returnsNullOnNull: null,
            stable: null,
            strict: null
        }
    }
];