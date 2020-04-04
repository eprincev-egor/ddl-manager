"use strict";

const body = `
begin
end
`;

module.exports = [
    {
        createdByDDLManager: false,
        filePath: "(database)",
        identify: "public.test_func(bigint)",
        name: "test_func",
        parsed: {
            schema: "public",
            name: "test_func",
            args: [
                {
                    default: null,
                    in: null,
                    name: "id",
                    out: null,
                    type: "bigint"
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