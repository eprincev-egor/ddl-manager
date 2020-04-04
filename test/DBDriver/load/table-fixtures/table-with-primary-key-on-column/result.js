"use strict";

module.exports = [
    {
        identify: "public.orders",
        filePath: "(database)",
        name: "orders",
        columns: [
            {
                filePath: null,
                identify: "id",
                key: "id",
                type: "integer",
                nulls: false,
                parsed: null,
                default: null
            }
        ],
        primaryKey: ["id"],
        
        deprecated: false,
        deprecatedColumns: [],
        values: null,
        checkConstraints: [],
        uniqueConstraints: [],
        foreignKeysConstraints: [],

        parsed: null
    }
];