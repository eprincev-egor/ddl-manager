"use strict";

module.exports = [
    {
        str: "ARRAY[]",
        result: {items: []}
    },
    {
        str: "arraY[]",
        result: {items: []}
    },
    {
        str: "array[1]",
        result: {items: [
            {elements: [
                {number: "1"}
            ]}
        ]}
    },
    {
        str: "array[ '' ]",
        result: {items: [
            {elements: [
                {content: ""}
            ]}
        ]}
    }
];
