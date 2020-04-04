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
                nulls: true,
                parsed: null
            },
            {
                filePath: null,
                identify: "profit",
                key: "profit",
                type: "integer",
                nulls: true,
                parsed: null
            }
        ],
        primaryKey: null,
        
        deprecated: false,
        deprecatedColumns: [],
        values: null,
        checkConstraints: [
            {
                filePath: null,
                identify: "orders_profit_check",
                name: "orders_profit_check",
                parsed: null
            }
        ],
        uniqueConstraints: [],
        foreignKeysConstraints: [],

        parsed: null
    }
];