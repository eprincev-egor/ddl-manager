"use strict";

module.exports = [
    {
        str: "substring('test' from 1)",
        result: {
            str: {elements: [{
                content: "test"
            }]},
            from: {elements: [{
                number: "1"
            }]}
        }
    },
    {
        str: "substring('test' for 2)",
        result: {
            str: {elements: [{
                content: "test"
            }]},
            for: {elements: [{
                number: "2"
            }]}
        }
    },
    {
        str: "substring('123456' from 2 for 3)",
        result: {
            str: {elements: [{
                content: "123456"
            }]},
            from: {elements: [{
                number: "2"
            }]},
            for: {elements: [{
                number: "3"
            }]}
        }
    }
];
