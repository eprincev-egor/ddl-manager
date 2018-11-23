"use strict";

module.exports = [
    {
        str: `
           create or replace function operation.content_func(a integer)
            returns integer as $body$select $1$body$
            language sql;
        `,
        result: {
            function: {
                schema: "operation",
                name: "content_func",
                language: "sql",
                args: [
                    {
                        name: "a",
                        type: "integer"
                    }
                ],
                returns: {
                    type: "integer"
                },
                body: {
                    content: "select $1"
                }
            }
        }
    },

    {
        str: `
           create or replace function operation.some_trigger()
            returns trigger as 
            $body$begin;return old;end$body$
            language plpgsql;

            create trigger super_duper
            after delete
            on operation.company
            for each row
            execute procedure operation.some_trigger()
        `,
        result: {
            function: {
                schema: "operation",
                name: "some_trigger",
                args: [],
                returns: {
                    type: "trigger"
                },
                language: "plpgsql",
                body: {
                    content: "begin;return old;end"
                }
            }
        }
    }
];
