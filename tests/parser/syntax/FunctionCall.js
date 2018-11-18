"use strict";

module.exports = [
    {
        str: "public.get_totals( company.id, 1 + 2 )",
        result: {
            "function": {link: [
                {word: "public"},
                {word: "get_totals"}
            ]},
            "arguments": [
                {
                    elements: [
                        {link: [
                            {word: "company"},
                            {word: "id"}
                        ]}
                    ]
                },
                {
                    elements: [
                        {number: "1"},
                        {operator: "+"},
                        {number: "2"}
                    ]
                }
            ]
        }
    }
];
