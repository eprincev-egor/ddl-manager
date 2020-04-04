"use strict";

module.exports = [
    {
        identify: "public.country",
        filePath: "(database)",
        name: "country",
        columns: [
            {
                filePath: null,
                identify: "id",
                key: "id",
                type: "integer",
                nulls: false,
                parsed: null,
                default: null
            },
            {
                filePath: null,
                identify: "code",
                key: "code",
                type: "text",
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
    },
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
                nulls: false,
                parsed: null,
                default: null
            },
            {
                filePath: null,
                identify: "id_country",
                key: "id_country",
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
        foreignKeysConstraints: [
            {
                filePath: null,
                identify: "company_id_country_fkey",
                name: "company_id_country_fkey",
                columns: ["id_country"],
                referenceTableIdentify: "public.country",
                referenceColumns: ["id"],
                parsed: null
            }
        ],

        parsed: null
    }
];