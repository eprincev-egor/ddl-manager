"use strict";

module.exports = [
    {
        identify: "public.test_serial",
        filePath: "(database)",
        name: "test_serial",
        columns: [
            {
                filePath: null,
                identify: "id",
                key: "id",
                type: "integer",
                nulls: false,
                parsed: null,
                default: "nextval('test_serial_id_seq'::regclass)"
            }
        ],
        primaryKey: null,
        
        deprecated: false,
        deprecatedColumns: [],
        values: null,
        checkConstraints: [],
        uniqueConstraints: [],
        foreignKeysConstraints: [],

        parsed: null
    },
    {
        identify: "public.test_smallserial",
        filePath: "(database)",
        name: "test_smallserial",
        columns: [
            {
                filePath: null,
                identify: "id",
                key: "id",
                type: "smallint",
                nulls: false,
                parsed: null,
                default: "nextval('test_smallserial_id_seq'::regclass)"
            }
        ],
        primaryKey: null,
        
        deprecated: false,
        deprecatedColumns: [],
        values: null,
        checkConstraints: [],
        uniqueConstraints: [],
        foreignKeysConstraints: [],

        parsed: null
    },
    {
        identify: "public.test_bigserial",
        filePath: "(database)",
        name: "test_bigserial",
        columns: [
            {
                filePath: null,
                identify: "id",
                key: "id",
                type: "bigint",
                nulls: false,
                parsed: null,
                default: "nextval('test_bigserial_id_seq'::regclass)"
            }
        ],
        primaryKey: null,
        
        deprecated: false,
        deprecatedColumns: [],
        values: null,
        checkConstraints: [],
        uniqueConstraints: [],
        foreignKeysConstraints: [],

        parsed: null
    }
];