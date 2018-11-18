"use strict";

module.exports = [
    {
        str: "+",
        result: {operator: "+"}
    },
    {
        str: ">= ",
        result: {operator: ">="}
    },
    {
        str: "<> ",
        result: {operator: "<>"}
    },
    {
        str: "and",
        result: {operator: "and"}
    },
    {
        str: "or",
        result: {operator: "or"}
    },
    {
        str: "not",
        result: {operator: "not"}
    },
    {
        str: "is",
        result: {operator: "is"}
    },
    {
        str: "is  Not null",
        result: {operator: "is not"}
    },
    {
        str: "is Distinct from",
        result: {operator: "is distinct from"}
    },
    {
        str: "operator( pg_catalog.+  )",
        result: {operator: "operator(pg_catalog.+)"}
    },
    {
        str: "isnull some",
        result: {operator: "isnull"}
    },
    {
        str: "notnull some",
        result: {operator: "notnull"}
    },
    {
        str: "is not distinct from",
        result: {operator: "is not distinct from"}
    },
    {
        str: "is unknown",
        result: {operator: "is unknown"}
    },
    {
        str: "is not  unknown",
        result: {operator: "is not unknown"}
    },
    {
        str: "sImilar  To",
        result: {operator: "similar to"}
    },
    {
        str: "iLike",
        result: {operator: "ilike"}
    },
    {
        str: "likE",
        result: {operator: "like"}
    }
];
