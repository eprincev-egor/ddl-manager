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
        identify: "public.test_func(bigint)",
        name: "test_func",
        parsed: {
            schema: "public",
            name: "test_func",
            args: [
                {
                    name: "id",
                    type: "bigint",
                    out: null,
                    in: null,
                    default: "null :: bigint"
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