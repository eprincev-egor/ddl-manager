"use strict";

module.exports = [
    {
        str: `create trigger test 
            before insert
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },
            
            before: true,
            insert: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            after insert
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },
            
            after: true,
            insert: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            before delete
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            before: true,
            delete: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            after delete
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            after: true,
            delete: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            before update
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },
            
            before: true,
            update: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            before insert or delete
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            before: true,
            insert: true,
            delete: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            after insert or delete
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            after: true,
            insert: true,
            delete: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            after delete or insert
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            after: true,
            insert: true,
            delete: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            before delete or insert
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            before: true,
            insert: true,
            delete: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            before delete or insert or update
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            before: true,
            insert: true,
            delete: true,
            update: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            after delete or insert or update
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            after: true,
            insert: true,
            delete: true,
            update: true,

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            after update of name, deleted
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            after: true,
            update: ["name", "deleted"],

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            after update of name, deleted or insert
            on public.company
            for each row
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            after: true,
            insert: true,
            update: ["name", "deleted"],

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    },
    {
        str: `create trigger test 
            after insert
            on public.company
            for each row
            when (pg_trigger_depth() = 0)
            execute procedure public.test()
        `,
        result: {
            name: "test",
            table: {
                schema: "public",
                name: "company"
            },

            after: true,
            insert: true,
            
            when: "pg_trigger_depth() = 0",

            procedure: {
                schema: "public",
                name: "test"
            }
        }
    }
];