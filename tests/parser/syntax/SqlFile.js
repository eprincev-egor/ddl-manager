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
            },
            triggers: [
                {
                    name: "super_duper",
                    table: {
                        schema: "operation",
                        name: "company"
                    },
        
                    after: true,
                    delete: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                }
            ]
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
            execute procedure operation.some_trigger();

            create trigger super_duper2
            after update
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
            },
            triggers: [
                {
                    name: "super_duper",
                    table: {
                        schema: "operation",
                        name: "company"
                    },
        
                    after: true,
                    delete: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                },
                {
                    name: "super_duper2",
                    table: {
                        schema: "operation",
                        name: "company"
                    },
        
                    after: true,
                    update: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                }
            ]
        }
    },

    {
        str: `
           create or replace function some_func()
            returns integer as $body$select 1$body$
            language sql;

            comment on function some_func() is $$test$$
        `,
        result: {
            function: {
                schema: "public",
                name: "some_func",
                language: "sql",
                args: [],
                returns: {
                    type: "integer"
                },
                body: {
                    content: "select 1"
                }
            },
            comments: [
                {
                    function: {
                        schema: "public",
                        name: "some_func",
                        args: []
                    },
                    comment: {
                        content: "test"
                    }
                }
            ]
        }
    },

    {
        str: `
           create or replace function some_func()
            returns integer as $body$select 1$body$
            language sql;

            comment on trigger some_trigger on company is $$test$$
        `,
        error: Error
    },

    {
        str: `
           create or replace function some_func()
            returns integer as $body$select 1$body$
            language sql;

            comment on function some_func1() is $$test$$
        `,
        error: Error
    },

    {
        str: `
           create or replace function public1.some_func()
            returns integer as $body$select 1$body$
            language sql;

            comment on function public2.some_func() is $$test$$
        `,
        error: Error
    },

    {
        str: `
           create or replace function some_func(x integer)
            returns integer as $body$select 1$body$
            language sql;

            comment on function some_func() is $$test$$
        `,
        error: Error
    },

    {
        str: `
           create or replace function operation.some_trigger()
            returns trigger as 
            $body$begin;return old;end$body$
            language plpgsql;

            comment on function operation.some_trigger() is $$c1$$;

            create trigger super_duper
            after delete
            on operation.company
            for each row
            execute procedure operation.some_trigger();

            comment on trigger super_duper on operation.company is $$c2$$;

            create trigger super_duper2
            after update
            on operation.company
            for each row
            execute procedure operation.some_trigger();

            comment on trigger super_duper2 on operation.company is $$c3$$
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
            },
            triggers: [
                {
                    name: "super_duper",
                    table: {
                        schema: "operation",
                        name: "company"
                    },
        
                    after: true,
                    delete: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                },
                {
                    name: "super_duper2",
                    table: {
                        schema: "operation",
                        name: "company"
                    },
        
                    after: true,
                    update: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                }
            ],
            comments: [
                {
                    function: {
                        schema: "operation",
                        name: "some_trigger",
                        args: []
                    },
                    comment: {
                        content: "c1"
                    }
                },
                {
                    trigger: {
                        name: "super_duper",
                        schema: "operation",
                        table: "company"
                    },
                    comment: {
                        content: "c2"
                    }
                },
                {
                    trigger: {
                        name: "super_duper2",
                        schema: "operation",
                        table: "company"
                    },
                    comment: {
                        content: "c3"
                    }
                }
            ]
        }
    },

    {
        str: `
           create or replace function operation.some_trigger()
            returns trigger as 
            $body$begin;return old;end$body$
            language plpgsql;

            comment on function operation.some_trigger() is $$c1$$;

            create trigger super_duper
            after delete
            on operation.company
            for each row
            execute procedure operation.some_trigger();

            create trigger super_duper2
            after update
            on operation.company
            for each row
            execute procedure operation.some_trigger();

            comment on trigger super_duper2 on operation.company is $$c3$$
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
            },
            triggers: [
                {
                    name: "super_duper",
                    table: {
                        schema: "operation",
                        name: "company"
                    },
        
                    after: true,
                    delete: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                },
                {
                    name: "super_duper2",
                    table: {
                        schema: "operation",
                        name: "company"
                    },
        
                    after: true,
                    update: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                }
            ],
            comments: [
                {
                    function: {
                        schema: "operation",
                        name: "some_trigger",
                        args: []
                    },
                    comment: {
                        content: "c1"
                    }
                },
                {
                    trigger: {
                        name: "super_duper2",
                        schema: "operation",
                        table: "company"
                    },
                    comment: {
                        content: "c3"
                    }
                }
            ]
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
            execute procedure operation.some_trigger();

            create trigger super_duper2
            after update
            on operation.company
            for each row
            execute procedure operation.some_trigger();

            comment on trigger super_duper2 on operation.company is $$c3$$
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
            },
            triggers: [
                {
                    name: "super_duper",
                    table: {
                        schema: "operation",
                        name: "company"
                    },
        
                    after: true,
                    delete: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                },
                {
                    name: "super_duper2",
                    table: {
                        schema: "operation",
                        name: "company"
                    },
        
                    after: true,
                    update: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                }
            ],
            comments: [
                {
                    trigger: {
                        name: "super_duper2",
                        schema: "operation",
                        table: "company"
                    },
                    comment: {
                        content: "c3"
                    }
                }
            ]
        }
    },

    {
        str: `
           create or replace function operation.some_trigger()
            returns trigger as 
            $body$begin;return old;end$body$
            language plpgsql;

            create trigger super_duper2
            after update
            on operation.company
            for each row
            execute procedure operation.some_trigger();

            comment on function super_duper2() is $$c3$$
        `,
        error: Error
    },

    {
        str: `
           create or replace function operation.some_trigger()
            returns trigger as 
            $body$begin;return old;end$body$
            language plpgsql;

            create trigger super_duper2
            after update
            on operation.company
            for each row
            execute procedure operation.some_trigger();

            comment on trigger super_duper on operation.company is $$c3$$
        `,
        error: Error
    },

    {
        str: `
           create or replace function operation.some_trigger()
            returns trigger as 
            $body$begin;return old;end$body$
            language plpgsql;

            create trigger super_duper2
            after update
            on operation.company
            for each row
            execute procedure operation.some_trigger();

            comment on trigger super_duper2 on company is $$c3$$
        `,
        error: Error
    },

    {
        str: `
           create or replace function operation.some_trigger()
            returns trigger as 
            $body$begin;return old;end$body$
            language plpgsql;

            create trigger super_duper2
            after update
            on operation.company
            for each row
            execute procedure operation.some_trigger();

            comment on trigger super_duper on operation.company1 is $$c3$$
        `,
        error: Error
    },

    {
        str: `
           create or replace function operation.some_trigger()
            returns trigger as 
            $body$begin;return old;end$body$
            language plpgsql;

            create trigger super_duper
            after update
            on operation.company
            for each row
            execute procedure operation.some_trigger();

            create trigger super_duper
            after update
            on operation.company
            for each row
            execute procedure operation.some_trigger();
        `,
        error: Error
    },

    
    {
        str: `
           create or replace function operation.some_trigger()
            returns trigger as 
            $body$begin;return old;end$body$
            language plpgsql;

            create trigger super_duper
            after insert
            on operation.company1
            for each row
            execute procedure operation.some_trigger();

            create trigger super_duper
            after insert
            on operation.company2
            for each row
            execute procedure operation.some_trigger();
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
            },
            triggers: [
                {
                    name: "super_duper",
                    table: {
                        schema: "operation",
                        name: "company1"
                    },
        
                    after: true,
                    insert: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                },
                {
                    name: "super_duper",
                    table: {
                        schema: "operation",
                        name: "company2"
                    },
        
                    after: true,
                    insert: true,
        
                    procedure: {
                        schema: "operation",
                        name: "some_trigger"
                    }
                }
            ]
        }
    }
];
