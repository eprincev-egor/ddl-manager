"use strict";

module.exports = [
    {
        str: "comment on trigger test on company is $$xxx$$",
        result: {
            trigger: {
                name: "test",
                schema: "public",
                table: "company"
            },
            comment: {
                content: "xxx"
            }
        }
    },
    {
        str: "comment on function test(integer, text) is $$yyy$$",
        result: {
            function: {
                name: "test",
                schema: "public",
                args: [
                    "integer",
                    "text"
                ]
            },
            comment: {
                content: "yyy"
            }
        }
    },
    {
        str: "comment on function operation.func() is $$yyy$$",
        result: {
            function: {
                name: "func",
                schema: "operation",
                args: []
            },
            comment: {
                content: "yyy"
            }
        }
    }
];
