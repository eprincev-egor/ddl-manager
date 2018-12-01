"use strict";

module.exports = [
    {
        str: "ID INTEGER",
        result: {
            name: "id",
            type: "integer"
        }
    },
    {
        str: "sum numeric ( 10, 3 )",
        result: {
            name: "sum",
            type: "numeric(10,3)"
        }
    },
    {
        str: "company_id bigint default null",
        options: {default: true},
        result: {
            name: "company_id",
            type: "bigint",
            default: "null"
        }
    },
    {
        str: "company_id bigint default null",
        options: {default: false},
        result: {
            name: "company_id",
            type: "bigint"
        }
    },
    {
        str: "company public.company",
        result: {
            name: "company",
            type: "public.company"
        }
    },
    {
        str: "company company",
        result: {
            name: "company",
            type: "public.company"
        }
    },
    {
        str: "text",
        result: {
            name: false,
            type: "text"
        }
    },
    {
        str: "timestamp without time zone",
        result: {
            name: false,
            type: "timestamp without time zone"
        }
    },
    {
        str: "double precision",
        result: {
            name: false,
            type: "double precision"
        }
    },
    {
        str: "character varying(2)",
        result: {
            name: false,
            type: "character varying(2)"
        }
    },
    {
        str: "time with time zone",
        result: {
            name: false,
            type: "time with time zone"
        }
    }
];
