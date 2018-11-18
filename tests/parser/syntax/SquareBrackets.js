"use strict";

module.exports = [
    {
        str: "[1]",
        result: {
            content: {elements: [
                {number: "1"}
            ]}
        }
    },
    {
        str: "[1 + 2]",
        result: {
            content: {elements: [
                {number: "1"},
                {operator: "+"},
                {number: "2"}
            ]}
        }
    }
];
