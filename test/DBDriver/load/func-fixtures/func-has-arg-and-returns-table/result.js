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
        identify: "public.test_func(text)",
        name: "test_func",
        parsed: {
            schema: "public",
            name: "test_func",
            args: [
                {
                    name: "nice",
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
                table: [
                    {
                        name: "x",
                        type: "text",
                        out: null,
                        in: null,
                        default: null
                    },
                    {
                        name: "y",
                        type: "integer",
                        out: null,
                        in: null,
                        default: null
                    }
                ],
                type: null
            },
            immutable: null,
            returnsNullOnNull: null,
            stable: null,
            strict: null
        }
    }
];