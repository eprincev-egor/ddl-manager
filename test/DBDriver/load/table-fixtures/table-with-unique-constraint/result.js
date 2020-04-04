"use strict";

module.exports = [
    {
        identify: "public.company",
        filePath: "(database)",
        name: "company",
        columns: [
            {
                filePath: null,
                identify: "id",
                key: "id",
                type: "integer",
                nulls: true,
                parsed: null,
                default: null
            },
            {
                filePath: null,
                identify: "name",
                key: "name",
                type: "text",
                nulls: true,
                parsed: null,
                default: null
            }
        ],
        primaryKey: null,
        
        deprecated: false,
        deprecatedColumns: [],
        values: null,
        checkConstraints: [],
        uniqueConstraints: [
            {
                filePath: null,
                identify: "company_unique_name",
                name: "company_unique_name",
                unique: ["name"],
                parsed: null
            }
        ],
        foreignKeysConstraints: [],

        parsed: null
    }
];