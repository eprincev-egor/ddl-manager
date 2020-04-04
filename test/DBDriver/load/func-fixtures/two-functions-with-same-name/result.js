"use strict";

const body1 = `
    begin
        raise notice 'test 1';
    end
`;
const body2 = `
    begin
        raise notice 'test 2';
    end
`;

module.exports = [
    {
        createdByDDLManager: false,
        filePath: "(database)",
        identify: "public.func_1(bigint)",
        name: "func_1",
        parsed: {
            schema: "public",
            name: "func_1",
            args: [
                {
                    default: null,
                    in: null,
                    name: "arg_1",
                    out: null,
                    type: "bigint"
                }
            ],
            body: {
                content: body1
            },
            comment: null,
            cost: null,
            language: "plpgsql",
            parallel: null,
            returns: {
                setof: null,
                table: null,
                type: "integer"
            },
            immutable: null,
            returnsNullOnNull: null,
            stable: null,
            strict: null
        }
    },
    {
        createdByDDLManager: false,
        filePath: "(database)",
        identify: "public.func_1(text)",
        name: "func_1",
        parsed: {
            schema: "public",
            name: "func_1",
            args: [
                {
                    default: null,
                    in: null,
                    name: "arg_1",
                    out: null,
                    type: "text"
                }
            ],
            body: {
                content: body2
            },
            comment: null,
            cost: null,
            language: "plpgsql",
            parallel: null,
            returns: {
                setof: null,
                table: null,
                type: "integer"
            },
            immutable: null,
            returnsNullOnNull: null,
            stable: null,
            strict: null
        }
    }
];