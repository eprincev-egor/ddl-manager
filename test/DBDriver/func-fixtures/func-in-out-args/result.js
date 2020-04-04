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
        identify: "public.some_func(integer)",
        name: "some_func",
        parsed: {
            schema: "public",
            name: "some_func",
            args: [
                {
                    name: "id",
                    type: "integer",
                    out: null,
                    in: null,
                    default: null
                },
                {
                    name: "name",
                    type: "text",
                    out: true,
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
                type: "text"
            },
            immutable: null,
            returnsNullOnNull: null,
            stable: null,
            strict: null
        }
    }
];