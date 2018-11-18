"use strict";

module.exports = [
    {
        str: "null",
        result: {null: true}
    },
    {
        str: "null ",
        result: {null: true}
    },
    {
        str: "null1",
        error: Error
    }
];
