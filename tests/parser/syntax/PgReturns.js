"use strict";

module.exports = [
    {
        str: "numerIc( 12 )",
        result: {
            type: "numeric(12)"
        }
    },
    {
        str: "table ( id integer , name text  )",
        result: {
            table: [
                {
                    name: "id",
                    type: "integer"
                },
                {
                    name: "name",
                    type: "text"
                }
            ]
        }
    }
];