"use strict";

module.exports = [
    {
        str: `create or replace function 
            TEST_NAME(xid integer, names text[])
            returns table(
                id integer, 
                sum numeric
            ) as $body$begin;end$body$
            language plpgsql;
        `,
        result: {
            schema: "public",
            name: "test_name",
            args: [
                {
                    name: "xid",
                    type: "integer"
                },
                {
                    name: "names",
                    type: "text[]"
                }
            ],
            returns: {
                table: [
                    {
                        name: "id",
                        type: "integer"
                    },
                    {
                        name: "sum",
                        type: "numeric"
                    }
                ]
            },
            body: {
                content: "begin;end"
            }
        }
    },
    {
        str: `create or replace function 
            TEST_NAME(id integer)
            returns table(
                id integer
            ) as $body$begin;end$body$
            language plpgsql;
        `,
        // parameter name "id" used more than once
        error: Error
    },
    {
        str: `create or replace function 
            TEST_NAME()
            returns table(
                id integer,
                id text
            ) as $body$begin;end$body$
            language plpgsql;
        `,
        // parameter name "id" used more than once
        error: Error
    },
    {
        str: `create or replace function 
            TEST_NAME(id integer, id text)
            returns integer as $body$begin;end$body$
            language plpgsql;
        `,
        // parameter name "id" used more than once
        error: Error
    },
    {
        str: `create or replace function 
            TEST_NAME(xid integer, names text[])
            returns table(
                id integer, 
                sum numeric
            ) 
            language plpgsql
            as $body$begin;end$body$
            ;
        `,
        result: {
            schema: "public",
            name: "test_name",
            args: [
                {
                    name: "xid",
                    type: "integer"
                },
                {
                    name: "names",
                    type: "text[]"
                }
            ],
            returns: {
                table: [
                    {
                        name: "id",
                        type: "integer"
                    },
                    {
                        name: "sum",
                        type: "numeric"
                    }
                ]
            },
            body: {
                content: "begin;end"
            }
        }
    },
    {
        str: `create or replace function TEST_NAME()
            returns trigger as $body$begin;end$body$
            language plpgsql;
        `,
        result: {
            schema: "public",
            name: "test_name",
            args: [],
            returns: {
                type: "trigger"
            },
            body: {
                content: "begin;end"
            }
        }
    },
    {
        str: `create Function TEST_NAME()
            returns trigger as $body$begin;end$body$
            language plpgsql;
        `,
        result: {
            schema: "public",
            name: "test_name",
            args: [],
            returns: {
                type: "trigger"
            },
            body: {
                content: "begin;end"
            }
        }
    },
    {
        str: `create Function TEST_NAME()
            returns void as $body$begin;end$body$
            language plpgsql;
        `,
        result: {
            schema: "public",
            name: "test_name",
            args: [],
            returns: {
                type: "void"
            },
            body: {
                content: "begin;end"
            }
        }
    },
    {
        str: `create Function TEST_NAME(
            a text default 'hi'
        )
            returns void as $body$begin;end$body$
            language plpgsql;
        `,
        result: {
            schema: "public",
            name: "test_name",
            args: [
                {
                    name: "a",
                    type: "text",
                    default: "'hi'"
                }
            ],
            returns: {
                type: "void"
            },
            body: {
                content: "begin;end"
            }
        }
    },
    {
        str: `create Function test(
            a text default null,
            b integer
        )
            returns void as $body$begin;end$body$
            language plpgsql;
        `,
        error: Error
    },
    {
        str: `create function test_sql_lang()
            returns integer as $body$select 1$body$
            language sql;
        `,
        result: {
            schema: "public",
            name: "test_sql_lang",
            language: "sql",
            args: [],
            returns: {
                type: "integer"
            },
            body: {
                content: "select 1"
            }
        }
    }
];
