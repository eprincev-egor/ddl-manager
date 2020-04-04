"use strict";

module.exports = [
    {
        createdByDDLManager: false,
        filePath: "(database)",
        tableIdentify: "public.test",
        functionIdentify: "public.test_func()",
        name: "test_trigger",
        identify: "test_trigger on public.test",
        parsed: {
            after: true,
            before: null,
            comment: null,
            constraint: null,
            deferrable: null,
            delete: true,
            initially: null,
            insert: true,
            name: "test_trigger",
            procedure: {
                args: [],
                name: "test_func",
                schema: "public"
            },
            statement: null,
            table: {
                name: "test",
                schema: "public"
            },
            update: true,
            updateOf: [
                "name",
                "note"
            ],
            when: null
        }
    }
];