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
                parsed: null
            },
            {
                filePath: null,
                identify: "name",
                key: "name",
                type: "text",
                nulls: true,
                parsed: null
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
                identify: "company_name_key",
                name: "company_name_key",
                unique: ["name"],
                parsed: null
            }
        ],
        foreignKeysConstraints: [],

        parsed: null
    }
];