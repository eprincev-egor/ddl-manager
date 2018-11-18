"use strict";

module.exports = [
    {
        str: "1",
        result: {number: "1"}
    },
    {
        str: "1234567890",
        result: {number: "1234567890"}
    },
    {
        str: "3.2",
        result: {number: "3.2"}
    },
    {
        str: "5e2",
        result: {number: "5e2"}
    },
    {
        str: ".001",
        result: {number: ".001"}
    },
    {
        str: "1.925e-3",
        result: {number: "1.925e-3"}
    },
    {
        str: "1.925e+3",
        result: {number: "1.925e+3"}
    }
];
